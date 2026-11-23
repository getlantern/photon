package http3

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	quic "github.com/getlantern/photon"
)

type tunnelCloseProbe struct {
	normal, abort atomic.Int32
	code          atomic.Uint64
}

func (*tunnelCloseProbe) Read([]byte) (int, error) { return 0, io.EOF }
func (p *tunnelCloseProbe) Close() error           { p.normal.Add(1); return nil }
func (p *tunnelCloseProbe) CloseWithError(code quic.StreamErrorCode, _ string) error {
	p.code.Store(uint64(code))
	p.abort.Add(1)
	return nil
}

func TestClientTunnelCloseAndAbortOrdering(t *testing.T) {
	for _, order := range []string{"close-first", "abort-first", "concurrent", "body-close-first"} {
		t.Run(order, func(t *testing.T) {
			source := new(tunnelCloseProbe)
			released := new(atomic.Int32)
			body := &reservedResponseBody{ReadCloser: source, release: func() { released.Add(1) }}
			input, output := io.Pipe()
			defer input.Close()
			tunnel := &Tunnel{reader: body, writer: output, ctx: context.Background()}
			closeNormal := func() { _ = tunnel.Close() }
			closeError := func() { _ = tunnel.CloseWithError(0x10e, "truncated") }
			switch order {
			case "close-first":
				closeNormal()
				closeError()
			case "abort-first":
				closeError()
				closeNormal()
			case "body-close-first":
				_ = body.Close()
				closeError()
			default:
				start := make(chan struct{})
				done := make(chan struct{}, 3)
				for _, closeFn := range []func(){closeNormal, closeError, func() { _ = body.Close() }} {
					go func() { <-start; closeFn(); done <- struct{}{} }()
				}
				close(start)
				<-done
				<-done
				<-done
			}
			_ = tunnel.CloseWithError(0x4321, "late")
			if source.normal.Load()+source.abort.Load() != 1 || released.Load() != 1 {
				t.Fatal("duplicate close/release", source.normal.Load(), source.abort.Load(), released.Load())
			}
			if order == "abort-first" && source.abort.Load() != 1 {
				t.Fatal("lost abort")
			}
			if (order == "close-first" || order == "body-close-first") && source.abort.Load() != 0 {
				t.Fatal("late abort changed result")
			}
			if source.abort.Load() != 0 && source.code.Load() != 0x10e {
				t.Fatal("wrong winning code", source.code.Load())
			}
			if _, err := tunnel.Write([]byte("late")); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
		})
	}
}
