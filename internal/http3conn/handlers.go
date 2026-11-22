package http3conn

import (
	"context"
	"errors"
	quic "github.com/getlantern/photon"
	"io"

	"github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

const (
	QPACKDecompressionFailed uint64 = 0x200
	QPACKEncoderStreamError  uint64 = 0x201
	QPACKDecoderStreamError  uint64 = 0x202
)

// Handlers parses known critical streams and delivers validated wire units to
// the connection owner. Callbacks run concurrently across streams and must
// serialize shared compression/settings state themselves. They must honor ctx
// and return typed HTTP/3 errors for semantic protocol violations. Encoder bytes
// are borrowed until the callback returns. No state queue is retained here.
type Handlers struct {
	PeerIsServer                                                bool
	MaxSettingsBytes, MaxSettingsEntries, MaxEncoderInstruction int
	Control                                                     func(context.Context, http3wire.ControlEvent) error
	Encoder                                                     func(context.Context, []byte) error
	Decoder                                                     func(context.Context, qpack.DecoderInstruction) error
	Push                                                        StreamHandler
}

func (h Handlers) Handler() (StreamHandler, error) {
	if h.MaxSettingsBytes < 0 || h.MaxSettingsEntries < 0 || h.MaxEncoderInstruction < 1 || h.Control == nil || h.Encoder == nil || h.Decoder == nil {
		return nil, errors.New("http3: invalid critical stream handlers")
	}
	return h.handle, nil
}

// sourceObserver distinguishes transport I/O failures from codec failures without
// changing the error itself or guessing from arbitrary error messages/types.
type sourceObserver struct {
	io.Reader
	failure error
}

func (s *sourceObserver) Read(p []byte) (int, error) {
	n, err := s.Reader.Read(p)
	if err != nil {
		s.failure = err
	}
	return n, err
}
func (h Handlers) handle(ctx context.Context, typ uint64, stream ReceiveStream) error {
	source := &sourceObserver{Reader: stream}
	switch typ {
	case http3wire.UniControl:
		reader, err := http3wire.NewControlReader(source, h.PeerIsServer, h.MaxSettingsBytes, h.MaxSettingsEntries)
		if err != nil {
			return err
		}
		for {
			event, err := reader.Next()
			if err != nil {
				if errors.Is(source.failure, quic.Err0RTTRejected) {
					return source.failure
				}
				return err
			}
			if err = h.Control(ctx, event); err != nil {
				return err
			}
		}
	case http3wire.UniQPACKEncoder:
		reader, err := qpack.NewEncoderInstructionReader(source, h.MaxEncoderInstruction)
		if err != nil {
			return err
		}
		for {
			wire, err := reader.Next()
			if err != nil {
				return criticalCodecError(err, source.failure, QPACKEncoderStreamError)
			}
			if err = h.Encoder(ctx, wire); err != nil {
				return err
			}
		}
	case http3wire.UniQPACKDecoder:
		reader, err := qpack.NewDecoderInstructionReader(source)
		if err != nil {
			return err
		}
		for {
			instruction, err := reader.Next()
			if err != nil {
				return criticalCodecError(err, source.failure, QPACKDecoderStreamError)
			}
			if err = h.Decoder(ctx, instruction); err != nil {
				return err
			}
		}
	case http3wire.UniPush:
		if h.Push == nil {
			return &http3wire.Error{Code: http3wire.IDError, Reason: "push stream without push permission"}
		}
		return h.Push(ctx, typ, stream)
	default:
		return &http3wire.Error{Code: http3wire.StreamCreationError, Reason: "unclassified stream dispatched"}
	}
}
func criticalCodecError(err, sourceErr error, code uint64) error {
	if errors.Is(sourceErr, quic.Err0RTTRejected) {
		return sourceErr
	}
	if sourceErr != nil && (errors.Is(err, sourceErr) || errors.Is(err, io.ErrUnexpectedEOF)) {
		return err
	}
	if errors.Is(err, qpack.ErrInstructionLimit) {
		return &http3wire.Error{Code: http3wire.ExcessiveLoad, Reason: err.Error()}
	}
	return &http3wire.Error{Code: code, Reason: err.Error()}
}
