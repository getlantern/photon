package http3

import (
	"errors"
	"fmt"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// ErrCode is an HTTP/3 application error code.
type ErrCode quic.ApplicationErrorCode

const (
	ErrCodeNoError ErrCode = 0x100 + iota
	ErrCodeGeneralProtocolError
	ErrCodeInternalError
	ErrCodeStreamCreationError
	ErrCodeClosedCriticalStream
	ErrCodeFrameUnexpected
	ErrCodeFrameError
	ErrCodeExcessiveLoad
	ErrCodeIDError
	ErrCodeSettingsError
	ErrCodeMissingSettings
	ErrCodeRequestRejected
	ErrCodeRequestCanceled
	ErrCodeRequestIncomplete
	ErrCodeMessageError
	ErrCodeConnectError
	ErrCodeVersionFallback
)

const (
	ErrCodeDatagramError            ErrCode = 0x33
	ErrCodeQPACKDecompressionFailed ErrCode = 0x200
)

func (c ErrCode) name() string {
	names := [...]string{
		"H3_NO_ERROR", "H3_GENERAL_PROTOCOL_ERROR", "H3_INTERNAL_ERROR",
		"H3_STREAM_CREATION_ERROR", "H3_CLOSED_CRITICAL_STREAM", "H3_FRAME_UNEXPECTED",
		"H3_FRAME_ERROR", "H3_EXCESSIVE_LOAD", "H3_ID_ERROR", "H3_SETTINGS_ERROR",
		"H3_MISSING_SETTINGS", "H3_REQUEST_REJECTED", "H3_REQUEST_CANCELLED",
		"H3_INCOMPLETE_REQUEST", "H3_MESSAGE_ERROR", "H3_CONNECT_ERROR", "H3_VERSION_FALLBACK",
	}
	if c >= ErrCodeNoError && c <= ErrCodeVersionFallback {
		return names[c-ErrCodeNoError]
	}
	if c == ErrCodeDatagramError {
		return "H3_DATAGRAM_ERROR"
	}
	if c == ErrCodeQPACKDecompressionFailed {
		return "QPACK_DECOMPRESSION_FAILED"
	}
	return ""
}

func (c ErrCode) String() string {
	if name := c.name(); name != "" {
		return name
	}
	return fmt.Sprintf("unknown error code: %#x", uint64(c))
}

// Error describes a local HTTP/3 protocol failure, stream cancellation or
// application connection close.
// Client request failures and response-body operations wrap underlying QUIC
// errors, which remain accessible through errors.Is and errors.As. Transport
// errors, timeouts and context cancellation are not converted.
// Server handler body, response and tunnel I/O use the same conversion.
type Error struct {
	Remote       bool
	ErrorCode    ErrCode
	ErrorMessage string
	cause        error
}

func (e *Error) Error() string {
	name := e.ErrorCode.name()
	if name == "" {
		name = fmt.Sprintf("H3 error (%#x)", uint64(e.ErrorCode))
	}
	if !e.Remote {
		name += " (local)"
	}
	if e.ErrorMessage != "" {
		name += ": " + e.ErrorMessage
	}
	return name
}

func (e *Error) Unwrap() error { return e.cause }

// Is compares code and endpoint; descriptive messages do not affect identity.
func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && other != nil && other.Remote == e.Remote && other.ErrorCode == e.ErrorCode
}

func httpError(err error) error {
	if err == nil {
		return nil
	}
	var existing *Error
	if errors.As(err, &existing) {
		return err
	}
	var stream *quic.StreamError
	if errors.As(err, &stream) {
		return &Error{Remote: stream.Remote, ErrorCode: ErrCode(stream.ErrorCode), cause: err}
	}
	var application *quic.ApplicationError
	if errors.As(err, &application) {
		return &Error{Remote: application.Remote, ErrorCode: ErrCode(application.ErrorCode), ErrorMessage: application.ErrorMessage, cause: err}
	}
	var close *quic.PeerCloseError
	if errors.As(err, &close) && close.Application {
		return &Error{Remote: true, ErrorCode: ErrCode(close.Code), ErrorMessage: close.Reason, cause: err}
	}
	var protocol *h3.Error
	if errors.As(err, &protocol) {
		return &Error{ErrorCode: ErrCode(protocol.Code), ErrorMessage: protocol.Reason, cause: err}
	}
	return err
}
