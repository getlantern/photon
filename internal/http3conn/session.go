package http3conn

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	"io"
	"sync"
	"sync/atomic"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	"github.com/getlantern/photon/internal/wire"
)

// SessionConfig supplies budgets and endpoint control/push policy. Control is
// called for non-SETTINGS events and must honor its context. LocalSettings must
// exactly match the settings already sent by OpenLocal. This is 1-RTT only.
// Callbacks must not call Stop synchronously (it joins those callbacks); cancel
// the parent context to initiate shutdown from a callback.
// DatagramSessionConfig transfers receive-loop and dispatcher lifetime to Session.
// The endpoint still owns request registration and FIN/reset propagation. Source
// must honor cancellation; AbortRequest must not synchronously call Session.Stop.
type DatagramSessionConfig struct {
	Dispatch     *DatagramDispatch
	Sender       DatagramSink
	Source       DatagramSource
	AbortRequest func(context.Context, uint64, uint64) error
}

type SessionConfig struct {
	// PeerSettings runs after successful live SETTINGS and encoder initialization.
	PeerSettings func(SettingsSnapshot)
	// EarlyPeerSettings enables client encoding before live SETTINGS. A non-nil
	// pointer to an empty slice uses defaults. The owner must bind these settings
	// to its offered ticket, and discard the entire session on early rejection.
	// This does not enable early CONNECT or HTTP datagrams.
	EarlyPeerSettings                                                             *[]h3.Setting
	Datagrams                                                                     *DatagramSessionConfig
	MaxRequests                                                                   int // bounded managed client requests; zero disables admission
	PeerIsServer                                                                  bool
	LocalSettings                                                                 []h3.Setting
	Decoder                                                                       qpack.DecoderLimits
	Encoder                                                                       EncoderLimits
	MaxActiveStreams, MaxSettingsBytes, MaxSettingsEntries, MaxEncoderInstruction int
	Control                                                                       func(context.Context, h3.ControlEvent) error
	Push                                                                          StreamHandler
}

// PeerGoaway identifies work the peer will not accept. RequestIDs is true for
// server GOAWAY (client-initiated bidirectional IDs), false for client GOAWAY
// (push IDs). Receipt prohibits ALL new requests/promises, even below ID.
// Already initiated work at or above ID is rejected; work below ID might have
// been processed and is not automatically safe to retry.
type PeerGoaway struct {
	ID         uint64
	RequestIDs bool
}

// Rejects classifies already initiated work in this snapshot's ID namespace.
func (g PeerGoaway) Rejects(id uint64) bool { return id >= g.ID }

// Session owns critical-stream processing and both QPACK directions after
// OpenLocal. Start exactly once per connection; no other code may read incoming
// unidirectional streams or write the local critical streams. Request
// streams, endpoint policy, connection shutdown and errors from request-side
// compression calls remain the endpoint owner's responsibility.
type Session struct {
	datagrams        *DatagramDispatch
	datagramSender   DatagramSink
	ctx              context.Context
	cancel           context.CancelCauseFunc
	done, ready      chan struct{}
	receivedSettings chan struct{}
	liveSettings     []h3.Setting
	settings         *SettingsState
	decoder          *FieldDecoder
	encoder          atomic.Pointer[FieldEncoder]
	goaway           atomic.Pointer[PeerGoaway]
	control          SendStream
	transport        IncomingTransport
	server           bool
	controlMu        sync.Mutex
	sentGoaway       bool
	sentLimit        uint64
	requestsMu       sync.Mutex
	requests         map[uint64]*RequestLease
	goawayReady      chan struct{}
	servingRequests  bool
	maxRequests      int
}

func StartSession(parent context.Context, transport IncomingTransport, local *LocalStreams, config SessionConfig) (*Session, error) {
	if config.MaxRequests < 0 || transport == nil || local == nil || local.Control == nil || local.Encoder == nil || local.Decoder == nil || config.MaxActiveStreams < 3 || config.Control == nil || config.MaxSettingsBytes < 0 || config.MaxSettingsEntries < 0 || config.MaxEncoderInstruction < 1 {
		return nil, fmt.Errorf("http3: invalid session configuration")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	var earlySettings []h3.Setting
	if config.EarlyPeerSettings != nil {
		if !config.PeerIsServer {
			return nil, fmt.Errorf("http3: remembered server settings on a server session")
		}
		earlySettings = append([]h3.Setting(nil), (*config.EarlyPeerSettings)...)
	}
	settings, err := NewSettingsState(!config.PeerIsServer, config.LocalSettings)
	if config.EarlyPeerSettings != nil {
		settings, err = NewEarlySettingsState(config.LocalSettings, earlySettings)
	}
	if err != nil {
		return nil, err
	}
	var datagrams DatagramSessionConfig
	if config.Datagrams != nil {
		datagrams = *config.Datagrams
		if datagrams.Dispatch == nil || datagrams.Source == nil || datagrams.AbortRequest == nil || !settings.Local().HTTPDatagrams {
			return nil, fmt.Errorf("http3: datagram receiver requires local advertisement and complete configuration")
		}
	}
	// Validate encoder budgets before publishing or touching critical writers.
	if _, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: ^uint64(0)}, config.Encoder, io.Discard); err != nil {
		return nil, err
	}
	decoder, err := settings.NewFieldDecoder(config.Decoder, local.Decoder)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	s := &Session{ctx: ctx, cancel: cancel, done: make(chan struct{}), ready: make(chan struct{}), receivedSettings: make(chan struct{}), settings: settings, decoder: decoder, control: local.Control, transport: transport, server: !config.PeerIsServer, maxRequests: config.MaxRequests}
	s.datagrams, s.datagramSender = datagrams.Dispatch, datagrams.Sender
	// Feedback can never be delivered again, so the session cannot continue. A
	// write interrupted by shutdown keeps the earlier cause. A reader of a
	// buffered response can get here before shutdown reaches the decoder, and
	// before the session has observed the close at all; a graceful close
	// answers both the same way shutdown does.
	decoder.feedbackFailed = func(err error) error {
		if ctx.Err() == nil {
			cancel(err)
			var peerClose *quic.PeerCloseError
			if !errors.As(err, &peerClose) && !errors.Is(err, quic.Err0RTTRejected) {
				code, reason := InternalError, "cannot write QPACK decoder feedback"
				var protocol *h3.Error
				if errors.As(err, &protocol) {
					code, reason = protocol.Code, protocol.Reason
				}
				transport.Abort(code, reason)
			}
		}
		return s.drained()
	}
	interrupt := func() {
		local.Control.SetWriteDeadline(time.Now())
		local.Encoder.SetWriteDeadline(time.Now())
		local.Decoder.SetWriteDeadline(time.Now())
	}
	interrupted := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() { defer close(interrupted); interrupt() })
	if config.EarlyPeerSettings != nil {
		encoder, err := NewEarlyFieldEncoder(earlySettings, config.Encoder, local.Encoder)
		if err != nil {
			cancel(err)
			if !stopInterrupt() {
				<-interrupted
			} else {
				interrupt()
			}
			decoder.Close(err)
			if !errors.Is(err, quic.Err0RTTRejected) && !errors.Is(context.Cause(ctx), quic.Err0RTTRejected) {
				transport.Abort(InternalError, "early encoder initialization failed")
			}
			if parent.Err() != nil {
				return nil, context.Cause(parent)
			}
			return nil, err
		}
		s.encoder.Store(encoder)
		close(s.ready)
	}
	handlers := Handlers{PeerIsServer: config.PeerIsServer, MaxSettingsBytes: config.MaxSettingsBytes, MaxSettingsEntries: config.MaxSettingsEntries, MaxEncoderInstruction: config.MaxEncoderInstruction, Push: config.Push,
		Control: func(ctx context.Context, event h3.ControlEvent) error {
			if event.Type != h3.Settings {
				if event.Type == h3.Goaway {
					// ControlReader has checked role, range and monotonicity.
					// Publish before notifying the request/push owner.
					s.receiveGoaway(PeerGoaway{ID: event.ID, RequestIDs: config.PeerIsServer})
				}
				return config.Control(ctx, event)
			}
			if err := settings.ApplyPeer(event.Settings); err != nil {
				return err
			}
			if encoder := s.encoder.Load(); encoder != nil {
				if err := encoder.ApplyPeerSettings(event.Settings); err != nil {
					return err
				}
				s.liveSettings = append([]h3.Setting(nil), event.Settings...)
				close(s.receivedSettings)
				if config.PeerSettings != nil {
					peer, _ := settings.Peer()
					config.PeerSettings(peer)
				}
				return nil
			}
			encoder, err := settings.NewFieldEncoder(config.Encoder, local.Encoder)
			if err != nil {
				return err
			}
			s.encoder.Store(encoder)
			s.liveSettings = append([]h3.Setting(nil), event.Settings...)
			close(s.receivedSettings)
			if config.PeerSettings != nil {
				peer, _ := settings.Peer()
				config.PeerSettings(peer)
			}
			close(s.ready)
			return nil
		},
		Encoder: func(_ context.Context, wire []byte) error { return decoder.EncoderInstruction(wire) },
		Decoder: func(_ context.Context, instruction qpack.DecoderInstruction) error {
			encoder := s.encoder.Load()
			if encoder == nil {
				// No field sections can be encoded before publication. A peer
				// may abandon a request before SETTINGS reaches us; cancellation
				// then has no references to release (RFC 9204 section 4.4.2).
				if instruction.Type == qpack.StreamCancellation {
					return nil
				}
				return &h3.Error{Code: QPACKDecoderStreamError, Reason: "decoder feedback before encoder initialization"}
			}
			return encoder.Feedback(instruction)
		}}
	handler, _ := handlers.Handler() // Configuration validated above.
	var datagramsDone chan struct{}
	if config.Datagrams != nil {
		datagramsDone = make(chan struct{})
		go func() {
			defer close(datagramsDone)
			defer datagrams.Dispatch.Close()
			err := datagrams.Dispatch.Run(ctx, datagrams.Source, datagrams.AbortRequest)
			cancel(err)
			var protocol *h3.Error
			if errors.As(err, &protocol) {
				transport.Abort(protocol.Code, protocol.Reason)
			}
		}()
	}
	go func() {
		defer close(s.done)
		err := RunIncoming(ctx, sessionIncoming{IncomingTransport: transport, cancel: cancel}, config.PeerIsServer, config.MaxActiveStreams, handler)
		cancel(err)
		if datagramsDone != nil {
			<-datagramsDone
		}
		if !stopInterrupt() {
			<-interrupted
		} else {
			interrupt()
		}
		s.controlMu.Lock()
		s.controlMu.Unlock() // Join an in-flight session control write after interruption.
		if cause := s.drained(); cause != nil {
			decoder.Detach(cause)
		} else {
			decoder.Close(context.Cause(ctx))
		}
		// The interrupted decoder stream fails any write still in progress.
		decoder.Flushed()
		s.requestsMu.Lock()
		clear(s.requests) // Request contexts were canceled through their session parent.
		s.requestsMu.Unlock()
	}()
	return s, nil
}

// drained returns the session's cause if it is a graceful peer close.
// ExchangeRequest keeps accepted responses readable after one; their buffered
// field sections still need the table state received so far.
func (s *Session) drained() error {
	if cause := context.Cause(s.ctx); gracefulPeerClose(cause) {
		return cause
	}
	return nil
}

// keptAfterClose reports whether a peer close leaves the response on request
// stream id readable: a graceful close does, unless a GOAWAY the session has
// read shows the peer did not process the request. QUIC can acknowledge the
// peer's GOAWAY before the control stream reader gets to it, so the close may
// be seen first. A response the transport holds complete was processed either
// way, and the transport refuses to read an incomplete one after the close.
func (s *Session) keptAfterClose(cause error, id uint64) bool {
	if !gracefulPeerClose(cause) {
		return false
	}
	goaway := s.goaway.Load()
	return goaway == nil || !goaway.Rejects(id)
}

// gracefulPeerClose reports a peer CONNECTION_CLOSE with H3_NO_ERROR.
func gracefulPeerClose(cause error) bool {
	var closed *quic.PeerCloseError
	return errors.As(cause, &closed) && closed.Application && closed.Code == 0x100
}

// WaitEncoder waits for successful initialization from live or remembered SETTINGS.
// Canceling this wait does not stop the connection session.
func (s *Session) WaitEncoder(ctx context.Context) (*FieldEncoder, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.ctx.Err(); err != nil {
		return nil, context.Cause(s.ctx)
	}
	select {
	case <-s.ready:
		if s.ctx.Err() != nil {
			return nil, context.Cause(s.ctx)
		}
		return s.encoder.Load(), nil
	case <-s.ctx.Done():
		return nil, context.Cause(s.ctx)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Settings is for snapshot/permission reads; only Session may apply peer settings.
func (s *Session) Settings() *SettingsState { return s.settings }
func (s *Session) Decoder() *FieldDecoder   { return s.decoder }
func (s *Session) Done() <-chan struct{}    { return s.done }

// Stop cancels and joins critical-stream work without sending a connection close.
// The endpoint must also shut down its transport. Stop may be called concurrently.
func (s *Session) Stop(cause error) { s.cancel(cause); <-s.done }

// Cancel the session before dispatcher joins: callbacks can be blocked writing
// QPACK feedback, so they need their write deadlines interrupted as well as reads.
type sessionIncoming struct {
	IncomingTransport
	cancel context.CancelCauseFunc
}

func (t sessionIncoming) Abort(code uint64, reason string) error {
	t.cancel(&h3.Error{Code: code, Reason: reason})
	return t.IncomingTransport.Abort(code, reason)
}
func (t sessionIncoming) AcceptUni(ctx context.Context) (ReceiveStream, error) {
	stream, err := t.IncomingTransport.AcceptUni(ctx)
	if err != nil {
		// Dispatcher cancellation can race its Abort call. Preserve the
		// protocol cause rather than winning that race with context.Canceled.
		cause := err
		if ctx.Err() != nil {
			cause = context.Cause(ctx)
		}
		t.cancel(cause)
	}
	return stream, err
}

// Goaway returns the latest received peer limit. Once present, an endpoint must
// stop initiating requests/promises and evaluate existing work against the limit.
// Managed BeginRequest leases are rejected before the control callback. The
// endpoint still owns stream allocation/reset, QPACK cancellation and retries. Later GOAWAY
// frames can lower the limit, so owners must process every callback.
func (s *Session) Goaway() (PeerGoaway, bool) {
	if state := s.goaway.Load(); state != nil {
		return *state, true
	}
	return PeerGoaway{}, false
}

var ErrGoawayID = errors.New("http3: invalid or increasing local GOAWAY identifier")

// SendGoaway serializes a GOAWAY frame on the already initialized control stream.
// Servers send client-bidirectional request IDs; clients send push IDs. Repeated
// or decreasing limits are permitted. Invalid local IDs return ErrGoawayID before
// writing and leave the session usable. Any write failure terminates the session
// and aborts the connection; uncertain control bytes are never retried.
// The session context interrupts blocked writes. The endpoint must coordinate
// admission with its chosen limit, cancel rejected work, and close after draining.
func (s *Session) SendGoaway(id uint64) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	if s.ctx.Err() != nil {
		return context.Cause(s.ctx)
	}
	if id > wire.MaxVarint || s.server && id%4 != 0 || s.sentGoaway && id > s.sentLimit {
		return ErrGoawayID
	}
	payload, _ := wire.AppendVarint(nil, id)
	frame, _ := h3.AppendHeader(nil, h3.Header{Type: h3.Goaway, Length: uint64(len(payload))})
	frame = append(frame, payload...)
	n, err := s.control.Write(frame)
	if n < 0 || n > len(frame) || err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if s.ctx.Err() != nil {
			return context.Cause(s.ctx)
		}
		s.cancel(err)
		s.transport.Abort(InternalError, "cannot write HTTP3 GOAWAY")
		return err
	}
	s.sentGoaway, s.sentLimit = true, id
	return nil
}

// ReceivedSettings closes only after validated live SETTINGS. Session shutdown
// and remembered early settings do not signal receipt.
func (s *Session) ReceivedSettings() <-chan struct{} { return s.receivedSettings }

// PeerSettings returns a private copy of the received wire settings. The wire
// parser's byte and entry budgets bound retention, including unknown settings.
func (s *Session) PeerSettings() ([]h3.Setting, bool) {
	select {
	case <-s.receivedSettings:
		return append([]h3.Setting(nil), s.liveSettings...), true
	default:
		return nil, false
	}
}
