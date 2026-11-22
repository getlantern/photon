package transport

import (
	"fmt"
	"net"
)

// ApplicationErrorCode is the application-defined CONNECTION_CLOSE code.
type ApplicationErrorCode uint64

// ApplicationError records the endpoint, code and reason for an application
// close. Its chain preserves legacy close errors; errors.Is also matches
// net.ErrClosed and application errors with the same endpoint and code.
type ApplicationError struct {
	Remote       bool
	ErrorCode    ApplicationErrorCode
	ErrorMessage string
	cause        error
}

func (e *ApplicationError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	text := fmt.Sprintf("Application error %#x (%s)", e.ErrorCode, side)
	if e.ErrorMessage != "" {
		text += ": " + e.ErrorMessage
	}
	return text
}
func (e *ApplicationError) Unwrap() error {
	if e.cause != nil {
		return e.cause
	}
	return net.ErrClosed
}
func (e *ApplicationError) Is(target error) bool {
	if target == net.ErrClosed {
		return true
	}
	other, ok := target.(*ApplicationError)
	return ok && other != nil && e.Remote == other.Remote && e.ErrorCode == other.ErrorCode
}
