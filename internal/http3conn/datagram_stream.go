package http3conn

import (
	"context"
	"errors"
	"net"

	quic "github.com/getlantern/photon"
)

// DatagramTransportStream provides independent send and receive completion.
type DatagramTransportStream interface {
	ClientExchangeStream
	ReceiveContext() context.Context
}

// DatagramStream binds a request's transport operations to datagram lifetime.
// It carries raw HTTP/3 bytes, not unframed tunnel DATA. The endpoint must validate
// request semantics before marking supported and must call Release on cleanup.
// Transport receive completion closes datagram reception without consuming bytes.
type DatagramStream struct {
	DatagramTransportStream
	session             *Session
	request             *DatagramRequest
	stopSendObserver    func() bool
	stopReceiveObserver func() bool
}

func (s *Session) BindDatagramStream(stream DatagramTransportStream, supported bool) (*DatagramStream, error) {
	return s.bindDatagramStream(stream, supported, false)
}

// BindPendingDatagramStream buffers bounded datagrams while request headers decode.
func (s *Session) BindPendingDatagramStream(stream DatagramTransportStream) (*DatagramStream, error) {
	return s.bindDatagramStream(stream, false, true)
}
func (s *Session) bindDatagramStream(stream DatagramTransportStream, supported, pending bool) (*DatagramStream, error) {
	if stream == nil || s.datagrams == nil {
		return nil, ErrRequestID
	}
	if s.ctx.Err() != nil {
		return nil, context.Cause(s.ctx)
	}
	request, err := s.datagrams.register(uint64(stream.StreamID()), supported, pending, stream.ReceiveContext(), func(code uint64) {
		stream.CancelRead(quic.StreamErrorCode(code))
		stream.CancelWrite(quic.StreamErrorCode(code))
	})
	if err != nil {
		return nil, err
	}
	bound := &DatagramStream{DatagramTransportStream: stream, session: s, request: request}
	bound.stopReceiveObserver = context.AfterFunc(stream.ReceiveContext(), request.CloseReceive)
	bound.stopSendObserver = context.AfterFunc(stream.Context(), bound.sendEnded)
	if stream.Context().Err() != nil {
		bound.sendEnded()
	}
	return bound, nil
}
func (s *DatagramStream) Read(p []byte) (int, error) {
	n, err := s.DatagramTransportStream.Read(p)
	if err != nil {
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			s.request.CloseReceive()
		}
	}
	return n, err
}
func (s *DatagramStream) Close() error {
	s.request.CloseSend()
	return s.DatagramTransportStream.Close()
}
func (s *DatagramStream) CancelWrite(code quic.StreamErrorCode) {
	s.request.CloseSend()
	s.DatagramTransportStream.CancelWrite(code)
}
func (s *DatagramStream) CancelRead(code quic.StreamErrorCode) {
	s.request.CloseReceive()
	s.DatagramTransportStream.CancelRead(code)
}
func (s *DatagramStream) Release(ctx context.Context) error {
	s.DetachDatagrams()
	return s.DatagramTransportStream.Release(ctx)
}
func (s *DatagramStream) SendDatagram(ctx context.Context, payload []byte) error {
	if s.Context().Err() != nil {
		s.sendEnded()
	}
	return s.session.SendHTTPDatagram(ctx, s.request, payload)
}
func (s *DatagramStream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return s.request.ReceiveContext(ctx)
}

func (s *DatagramStream) sendEnded() {
	cause := context.Cause(s.Context())
	var reset *quic.StreamError
	if !errors.As(cause, &reset) {
		cause = ErrDatagramSendClosed
	}
	s.request.cancelSend(cause)
}

// DetachDatagrams releases HTTP datagram state without waiting for transport
// acknowledgments. Owners using automatic QUIC stream release call this at final
// request cleanup. It is safe to repeat.
func (s *DatagramStream) DetachDatagrams() {
	s.stopSendObserver()
	s.stopReceiveObserver()
	s.request.Remove()
}

// CloseDatagramSend precedes an asynchronously queued transport FIN.
func (s *DatagramStream) CloseDatagramSend() { s.request.CloseSend() }

func (s *DatagramStream) ResolveDatagrams(supported bool) error { return s.request.resolve(supported) }
func (s *DatagramStream) DatagramAbortContext() context.Context { return s.request.abortContext }

// SetPriority forwards scheduling updates through the datagram lifetime wrapper.
func (s *DatagramStream) SetPriority(urgency int8, incremental bool) {
	if scheduler, ok := s.DatagramTransportStream.(interface{ SetPriority(int8, bool) }); ok {
		scheduler.SetPriority(urgency, incremental)
	}
}
