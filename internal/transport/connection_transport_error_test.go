package transport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/wire"
)

func TestConnectionFailureContract(t *testing.T) {
	for _, test := range []struct {
		cause error
		code  TransportErrorCode
		frame uint64
	}{
		{&TransportError{Code: 7, FrameType: 8, Reason: "bad frame"}, 7, 8},
		{wire.ErrEarlyLimits, 0xa, 0},
		{wire.ErrTransportParameter, 8, 0},
		{handshake.ErrCryptoBuffer, 0xd, 6},
		{handshake.ErrCryptoConflict, 0xa, 6},
		{fmt.Errorf("TLS: %w", tls.AlertError(42)), 0x12a, 0},
	} {
		got := connectionFailure(test.cause)
		var failure *ConnectionTransportError
		if !errors.As(got, &failure) || failure.Remote || failure.ErrorCode != test.code || failure.FrameType != test.frame || !errors.Is(got, test.cause) || !errors.Is(got, net.ErrClosed) || connectionFailure(got) != got {
			t.Fatal(got)
		}
		if !errors.Is(got, &ConnectionTransportError{ErrorCode: test.code, FrameType: test.frame}) || errors.Is(got, &ConnectionTransportError{Remote: true, ErrorCode: test.code, FrameType: test.frame}) || errors.Is(got, &ConnectionTransportError{ErrorCode: test.code, FrameType: test.frame + 1}) {
			t.Fatal("identity", got)
		}
	}
	for _, cause := range []error{ErrLocalClosed, ErrStatelessReset, &ApplicationError{ErrorCode: 8}, &IdleTimeoutError{}} {
		if connectionFailure(cause) != cause {
			t.Fatal("unrelated failure converted", cause)
		}
	}
	if TransportErrorCode(0xff).IsCryptoError() || !TransportErrorCode(0x100).IsCryptoError() || !TransportErrorCode(0x1ff).IsCryptoError() || TransportErrorCode(0x200).IsCryptoError() {
		t.Fatal("crypto bounds")
	}
	if TransportErrorCode(0x12a).Message() != tls.AlertError(42).Error() || TransportErrorCode(1<<40).String() != "unknown error code: 0x10000000000" {
		t.Fatal("code formatting")
	}
}
