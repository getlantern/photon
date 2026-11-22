package http3conn

import (
	"fmt"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

type SectionKind uint8

const (
	InitialSection SectionKind = iota
	InformationalSection
	TrailerSection
)

type MessageConfig struct {
	CapsuleProtocols map[string]bool // request-side policy selected by validated :protocol
	Capsules         bool            // explicit extension policy; not inferred from the header
	RequestProtocol  string          // selected Extended CONNECT token for responses
	Kind             FieldContext
	RequestMethod    string // required when receiving a response
	ExtendedConnect  bool   // negotiated permission, not just local support
	Push             bool   // response received on a push stream
}

// Message receives one direction of a request stream (or a pushed response).
// Feed decoded HEADERS in wire order, actual DATA payload bytes before delivery,
// and other frame types after parsing their headers. Finish represents clean FIN,
// not a reset or truncated frame. QPACK blocking must preserve that ordering.
// Protocol errors are sticky: *http3wire.Error closes the connection, whereas
// *MessageError resets the request stream. This type does not perform either I/O.
// Calls must be serialized. Resource limits, push-ID validation, unknown payload
// skipping, and extension semantics belong to the connection/stream owner.
type Message struct {
	config                     MessageConfig
	body                       *BodyLength
	control                    ControlData
	trailers, finished, tunnel bool
	err                        error
}

func NewMessage(c MessageConfig) (*Message, error) {
	if c.Kind != RequestFields && c.Kind != ResponseFields || c.Push && c.Kind != ResponseFields || c.Kind == ResponseFields && !validToken(c.RequestMethod) {
		return nil, fmt.Errorf("http3: invalid message configuration")
	}
	return &Message{config: c}, nil
}
func (m *Message) Headers(fields []qpack.DecodedField) (SectionKind, ControlData, error) {
	if m.err != nil {
		return 0, ControlData{}, m.err
	}
	if m.finished || m.trailers || m.tunnel {
		return 0, ControlData{}, m.unexpected("HEADERS after trailers, FIN or established CONNECT")
	}
	kind := m.config.Kind
	if m.body != nil {
		kind = TrailerFields
	}
	control, err := ValidateControlData(fields, kind, m.config.ExtendedConnect)
	if err != nil {
		return 0, ControlData{}, m.fail(err)
	}
	if kind == TrailerFields {
		if m.config.Kind == ResponseFields && (m.control.Status == 204 || m.control.Status == 304) {
			return 0, ControlData{}, m.fail(&MessageError{"trailers prohibited for response status"})
		}
		if err := validateTrailerRouting(fields); err != nil {
			return 0, ControlData{}, m.fail(err)
		}
		if err := m.body.Finish(); err != nil {
			return 0, ControlData{}, m.fail(err)
		}
		m.trailers = true
		return TrailerSection, control, nil
	}
	if m.config.Capsules || kind == RequestFields && m.config.CapsuleProtocols[control.Protocol] {
		if err := validateCapsuleFields(fields, control, m.config); err != nil {
			return 0, ControlData{}, m.fail(err)
		}
	}
	body, err := NewBodyLength(fields, control, kind, m.config.RequestMethod)
	if err != nil {
		return 0, ControlData{}, m.fail(err)
	}
	if kind == ResponseFields && control.Status < 200 {
		return InformationalSection, control, nil
	}
	m.body, m.control = body, control
	m.tunnel = kind == ResponseFields && body.tunnel
	return InitialSection, control, nil
}
func (m *Message) Data(n uint64) error {
	if m.err != nil {
		return m.err
	}
	if m.body == nil || m.trailers || m.finished {
		return m.unexpected("DATA before final headers or after trailers/FIN")
	}
	if err := m.body.Add(n); err != nil {
		return m.fail(err)
	}
	return nil
}

// OtherFrame checks base-protocol placement only. A successful PUSH_PROMISE check
// still requires payload/QPACK/push-ID validation by the owner. Unknown frame
// payloads must be consumed without changing message state.
func (m *Message) OtherFrame(typ uint64) error {
	if m.err != nil {
		return m.err
	}
	if m.finished {
		return m.unexpected("frame after FIN")
	}
	switch typ {
	case h3.Headers, h3.Data:
		return fmt.Errorf("http3: use Headers or Data for message frames")
	case h3.PushPromise:
		if m.config.Kind != ResponseFields || m.config.Push || m.tunnel {
			return m.unexpected("PUSH_PROMISE on forbidden stream")
		}
	case h3.Settings, h3.CancelPush, h3.Goaway, h3.MaxPushID, h3.PriorityUpdateRequest, h3.PriorityUpdatePush, 2, 6, 8, 9:
		return m.unexpected("control or reserved HTTP/2 frame on message stream")
	}
	return nil
}

// EstablishTunnel is called on the request direction when its paired successful
// CONNECT response is sent. The response direction switches automatically.
func (m *Message) EstablishTunnel() error {
	if m.err != nil {
		return m.err
	}
	if m.body == nil || m.config.Kind != RequestFields || m.control.Method != "CONNECT" {
		return fmt.Errorf("http3: no CONNECT request to establish")
	}
	if m.trailers {
		return m.unexpected("trailers on established CONNECT")
	}
	m.tunnel = true
	return nil
}
func (m *Message) Finish() error {
	if m.err != nil {
		return m.err
	}
	if m.finished {
		return nil
	}
	if m.body == nil {
		return m.fail(&MessageError{"stream ended without request or final response headers"})
	}
	if err := m.body.Finish(); err != nil {
		return m.fail(err)
	}
	m.finished = true
	return nil
}
func (m *Message) unexpected(reason string) error {
	return m.fail(&h3.Error{Code: h3.FrameUnexpected, Reason: reason})
}
func (m *Message) fail(err error) error { m.err = err; return err }
