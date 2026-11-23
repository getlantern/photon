package quic

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

func TestStatelessResetErrorContract(t *testing.T) {
	original := &StatelessResetError{}
	err := fmt.Errorf("receive failed: %w", original)
	var reset *StatelessResetError
	var network net.Error
	var protocol *TransportError
	if !errors.As(err, &reset) || reset != original || !errors.As(err, &network) || network.Timeout() || !network.Temporary() || !errors.Is(err, net.ErrClosed) || errors.As(err, &protocol) {
		t.Fatal(err)
	}
}
