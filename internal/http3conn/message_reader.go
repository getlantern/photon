package http3conn

import (
	"context"
	"errors"
	"fmt"
	"io"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// MessageEvent describes one frame. HEADERS are consumed and decoded by Next;
// Section, Control and Fields are meaningful only for HEADERS. For other frames,
// consume the payload through Read before calling Next again. Unknown frames and
// PUSH_PROMISE remain visible so the owner can apply extension/push policy.
type MessageEvent struct {
	Header  h3.Header
	Section SectionKind
	Control ControlData
	Fields  []qpack.DecodedField
}

// MessageReader combines bounded HEADERS decoding with streaming message
// validation. Calls must be serialized. Errors are sticky except local payload
// backpressure and an expired stream deadline, after which the same call resumes
// where it stopped. The owner maps errors to stream/connection closure, cancels the
// FieldStream on abandonment, and uses transport deadlines to interrupt reads.
// Next's context controls QPACK blocking, not arbitrary source I/O. Successful
// EOF from Next means a clean FIN and a valid complete message; EOF from Read
// means only the current payload ended. PUSH_PROMISE payload/QPACK validation
// and CONNECT request tunnel establishment remain owner responsibilities.
type MessageReader struct {
	frames     *h3.Reader
	fields     *FieldStream
	message    *Message
	maxHeaders uint64
	data       bool
	headers    *h3.Header // HEADERS frame whose block is still being read
	block      []byte
	err        error
}

func NewMessageReader(source io.Reader, fields *FieldStream, config MessageConfig, maxEncodedHeaders uint64) (*MessageReader, error) {
	if source == nil || fields == nil || maxEncodedHeaders == 0 || maxEncodedHeaders > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("http3: invalid message reader configuration")
	}
	message, err := NewMessage(config)
	if err != nil {
		return nil, err
	}
	return &MessageReader{frames: h3.NewReader(source), fields: fields, message: message, maxHeaders: maxEncodedHeaders}, nil
}

func (r *MessageReader) Next(ctx context.Context) (MessageEvent, error) {
	if r.err != nil {
		return MessageEvent{}, r.err
	}
	if err := ctx.Err(); err != nil {
		return MessageEvent{}, r.fail(err)
	}
	var header h3.Header
	if r.headers != nil {
		header = *r.headers
	} else {
		var err error
		header, err = r.frames.Next()
		if errors.Is(err, h3.ErrPayloadPending) || h3.IsDeadline(err) {
			return MessageEvent{}, err
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if e := r.message.Finish(); e != nil {
					err = e
				}
			}
			return MessageEvent{}, r.fail(err)
		}
	}
	event := MessageEvent{Header: header}
	r.data = header.Type == h3.Data
	switch header.Type {
	case h3.Headers:
		if header.Length > r.maxHeaders {
			return MessageEvent{}, r.fail(&h3.Error{Code: h3.ExcessiveLoad, Reason: "encoded HEADERS exceeds local limit"})
		}
		r.headers = &header
		for {
			if len(r.block) == cap(r.block) {
				r.block = append(r.block, 0)[:len(r.block)]
			}
			n, err := r.frames.Read(r.block[len(r.block):cap(r.block)])
			r.block = r.block[:len(r.block)+n]
			if err == io.EOF {
				break
			}
			if h3.IsDeadline(err) {
				return MessageEvent{}, err
			}
			if err != nil {
				return MessageEvent{}, r.fail(err)
			}
		}
		block := r.block
		r.headers, r.block = nil, nil
		var err error
		event.Fields, err = r.fields.Decode(ctx, block)
		if err != nil {
			return MessageEvent{}, r.fail(err)
		}
		event.Section, event.Control, err = r.message.Headers(event.Fields)
		if err != nil {
			return MessageEvent{}, r.fail(err)
		}
	case h3.Data:
		// Even empty DATA frames are subject to message ordering.
		if err := r.message.Data(0); err != nil {
			return MessageEvent{}, r.fail(err)
		}
	default:
		if err := r.message.OtherFrame(header.Type); err != nil {
			return MessageEvent{}, r.fail(err)
		}
	}
	return event, nil
}

func (r *MessageReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	n, err := r.frames.Read(p)
	if r.data && n > 0 {
		if e := r.message.Data(uint64(n)); e != nil {
			return 0, r.fail(e)
		}
	}
	if err != nil && !errors.Is(err, io.EOF) && !h3.IsDeadline(err) {
		return n, r.fail(err)
	}
	return n, err
}

// EstablishTunnel switches an already received CONNECT request after its paired
// successful response is sent. Successful CONNECT responses switch automatically.
func (r *MessageReader) EstablishTunnel() error {
	if r.err != nil {
		return r.err
	}
	return r.message.EstablishTunnel()
}
func (r *MessageReader) fail(err error) error { r.err = err; return err }
