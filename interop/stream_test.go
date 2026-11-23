package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/getlantern/photon/internal/stream"
	engine "github.com/getlantern/photon/internal/transport"
	upstream "github.com/quic-go/quic-go"
)

// exchangeStream exercises both flow-control directions using payloads larger
// than either endpoint's fixed receive windows. The independent engine is
// accessed only by its serialized UDP driver; the upstream application runs
// concurrently through upstream's public stream API.
func exchangeStream(ctx context.Context, c *engine.Conn, socket *net.UDPConn, peer *net.UDPAddr, up *upstream.Conn) error {
	request := bytes.Repeat([]byte("independent request\n"), 2048)
	response := bytes.Repeat([]byte("upstream response\n"), 2048)
	peerDone := make(chan error, 1)
	go func() {
		peerDone <- func() error {
			s, err := up.AcceptStream(ctx)
			if err != nil {
				return err
			}
			if deadline, ok := ctx.Deadline(); ok {
				s.SetDeadline(deadline)
			}
			received, err := io.ReadAll(io.LimitReader(s, int64(len(request)+1)))
			if err != nil {
				return err
			}
			if !bytes.Equal(received, request) {
				return fmt.Errorf("upstream request mismatch: %d bytes", len(received))
			}
			if n, err := s.Write(response); err != nil {
				return err
			} else if n != len(response) {
				return io.ErrShortWrite
			}
			return s.Close()
		}()
	}()
	id, err := c.OpenStream(false)
	if err != nil {
		return err
	}
	sent := 0
	var received []byte
	finished := false
	progress := func() error {
		for sent < len(request) {
			n, err := c.WriteStream(id, request[sent:], true)
			if errors.Is(err, stream.ErrWouldBlock) {
				break
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrNoProgress
			}
			sent += n
		}
		if finished {
			return nil
		}
		for {
			buffer := make([]byte, 1000)
			n, err := c.ReadStream(id, buffer)
			received = append(received, buffer[:n]...)
			if len(received) > len(response) {
				return errors.New("response exceeds expected length")
			}
			if errors.Is(err, stream.ErrWouldBlock) {
				return nil
			}
			if err == io.EOF {
				finished = true
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	if err := driveProgress(ctx, c, socket, peer, progress, func() bool { return finished }); err != nil {
		window, flight := c.CongestionStats()
		return fmt.Errorf("driver: %w (sent=%d/%d received=%d/%d cwnd=%d flight=%d outstanding=%v)", err, sent, len(request), len(received), len(response), window, flight, c.HasOutstanding())
	}
	if !bytes.Equal(received, response) {
		return fmt.Errorf("independent response mismatch: %d bytes", len(received))
	}
	select {
	case err := <-peerDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
