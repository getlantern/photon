package transport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/wire"
)

type TransportErrorCode uint64

func (c TransportErrorCode) IsCryptoError() bool { return c >= 0x100 && c < 0x200 }
func (c TransportErrorCode) Message() string {
	if c.IsCryptoError() {
		return tls.AlertError(c - 0x100).Error()
	}
	return ""
}
func (c TransportErrorCode) String() string {
	names := [...]string{"NO_ERROR", "INTERNAL_ERROR", "CONNECTION_REFUSED", "FLOW_CONTROL_ERROR", "STREAM_LIMIT_ERROR", "STREAM_STATE_ERROR", "FINAL_SIZE_ERROR", "FRAME_ENCODING_ERROR", "TRANSPORT_PARAMETER_ERROR", "CONNECTION_ID_LIMIT_ERROR", "PROTOCOL_VIOLATION", "INVALID_TOKEN", "APPLICATION_ERROR", "CRYPTO_BUFFER_EXCEEDED", "KEY_UPDATE_ERROR", "AEAD_LIMIT_REACHED", "NO_VIABLE_PATH"}
	if c < TransportErrorCode(len(names)) {
		return names[c]
	}
	if c.IsCryptoError() {
		return fmt.Sprintf("CRYPTO_ERROR %#x", uint64(c))
	}
	return fmt.Sprintf("unknown error code: %#x", uint64(c))
}

// ConnectionTransportError is the public connection-level transport failure.
// The original engine/TLS error remains available through its error chain.
type ConnectionTransportError struct {
	Remote       bool
	FrameType    uint64
	ErrorCode    TransportErrorCode
	ErrorMessage string
	cause        error
}

func (e *ConnectionTransportError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	text := fmt.Sprintf("%s (%s)", e.ErrorCode, side)
	if e.FrameType != 0 {
		text += fmt.Sprintf(" (frame type: %#x)", e.FrameType)
	}
	message := e.ErrorMessage
	if message == "" {
		message = e.ErrorCode.Message()
	}
	if message != "" {
		text += ": " + message
	}
	return text
}
func (e *ConnectionTransportError) Unwrap() []error { return []error{net.ErrClosed, e.cause} }
func (e *ConnectionTransportError) Is(target error) bool {
	other, ok := target.(*ConnectionTransportError)
	return ok && other != nil && e.Remote == other.Remote && e.FrameType == other.FrameType && e.ErrorCode == other.ErrorCode
}

func connectionFailure(err error) error {
	if err == nil {
		return nil
	}
	var existing *ConnectionTransportError
	if errors.As(err, &existing) {
		return err
	}
	result := &ConnectionTransportError{cause: err}
	var local *TransportError
	var alert tls.AlertError
	switch {
	case errors.As(err, &local):
		result.ErrorCode = TransportErrorCode(local.Code)
		result.FrameType = local.FrameType
		result.ErrorMessage = local.Reason
	case errors.Is(err, wire.ErrEarlyLimits):
		result.ErrorCode = 0xa
		result.ErrorMessage = "incompatible accepted early-data limits"
	case errors.Is(err, wire.ErrTransportParameter):
		result.ErrorCode = 8
		result.ErrorMessage = "invalid transport parameters"
	case errors.Is(err, handshake.ErrCryptoBuffer):
		result.ErrorCode = 0xd
		result.FrameType = 6
		result.ErrorMessage = "CRYPTO buffer exceeded"
	case errors.Is(err, handshake.ErrCryptoConflict):
		result.ErrorCode = 0xa
		result.FrameType = 6
		result.ErrorMessage = "conflicting CRYPTO data"
	case errors.As(err, &alert):
		result.ErrorCode = 0x100 + TransportErrorCode(alert)
		result.ErrorMessage = err.Error()
	default:
		return err
	}
	return result
}
