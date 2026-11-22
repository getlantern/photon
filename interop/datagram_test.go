package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/getlantern/photon/internal/stream"
	engine "github.com/getlantern/photon/internal/transport"
	upstream "github.com/quic-go/quic-go"
)

func exchangeDatagrams(ctx context.Context, c *engine.Conn, socket *net.UDPConn, peer *net.UDPAddr, up *upstream.Conn) error {
	payloads := [][]byte{nil, []byte("independent QUIC datagram"), bytes.Repeat([]byte{0xa5}, 1158)}
	done := make(chan error, 1)
	go func() {
		for _, want := range payloads {
			got, err := up.ReceiveDatagram(ctx)
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(got, want) {
				done <- fmt.Errorf("upstream datagram mismatch")
				return
			}
			if err := up.SendDatagram(got); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	received := 0
	for _, data := range payloads {
		if err := c.SendDatagram(data); err != nil {
			return err
		}
	}
	progress := func() error {
		for received < len(payloads) {
			got, err := c.ReceiveDatagram()
			if errors.Is(err, stream.ErrWouldBlock) {
				return nil
			}
			if err != nil {
				return err
			}
			if !bytes.Equal(got, payloads[received]) {
				return fmt.Errorf("independent datagram mismatch")
			}
			received++
		}
		return nil
	}
	if err := driveProgress(ctx, c, socket, peer, progress, func() bool { return received == len(payloads) }); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
