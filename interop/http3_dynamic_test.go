package interop

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3conn "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
)

// Handcrafted QPACK wire over an independent QUIC transport, not an independent
// HTTP/3/QPACK implementation. HEADERS deliberately precedes its insertion.
func TestHTTP3LiveBlockedQPACK(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, canceled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/canceled=%t", uint32(version), canceled), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				clientTLS.NextProtos = []string{"h3"}
				serverTLS.NextProtos = []string{"h3"}
				listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				deadline, _ := ctx.Deadline()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				conn, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.CloseWithError(0, "")
				local, err := h3conn.OpenLocal(ctx, h3conn.QUICTransport{Conn: conn}, liveHTTP3Settings())
				if err != nil {
					t.Fatal(err)
				}
				control, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				control.SetWriteDeadline(deadline)
				if _, err = control.Write([]byte{0, 4, 0}); err != nil {
					t.Fatal(err)
				}
				_, _, decoder := startHTTP3Dispatch(t, ctx, conn, false, local)
				stream, err := peer.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream.SetWriteDeadline(deadline)
				// HEADERS length 3, RIC=1, Base=1, dynamic relative index zero.
				if _, err = stream.Write([]byte{1, 3, 2, 0, 0x80}); err != nil {
					t.Fatal(err)
				}
				incoming, err := conn.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				incoming.SetReadDeadline(deadline)
				frames := h3.NewReader(incoming)
				header, err := frames.Next()
				if err != nil || header.Type != h3.Headers {
					t.Fatal(header, err)
				}
				section, err := io.ReadAll(frames)
				if err != nil {
					t.Fatal(err)
				}
				ready, err := decoder.Submit(uint64(incoming.StreamID()), section)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-ready:
					t.Fatal("section completed before insertion")
				default:
				}
				if canceled {
					owner, err := h3conn.NewFieldStream(decoder, uint64(incoming.StreamID()))
					if err != nil {
						t.Fatal(err)
					}
					// Direct Submit above deliberately establishes the blocked state
					// before exercising the request owner's idempotent cancellation.
					if err := owner.Cancel(); err != nil {
						t.Fatal(err)
					}
					if err := owner.Cancel(); err != nil {
						t.Fatal(err)
					}
					select {
					case result := <-ready:
						if result.Err != h3conn.ErrFieldSectionCanceled {
							t.Fatal(result)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if _, err := owner.Decode(ctx, []byte{0, 0}); err != h3conn.ErrFieldSectionCanceled {
						t.Fatal(err)
					}
				} else {
					encoder, err := peer.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					encoder.SetWriteDeadline(deadline)
					// Encoder stream prefix; capacity 128; literal insertion x: y.
					if _, err = encoder.Write([]byte{2, 0x3f, 0x61, 0x41, 'x', 1, 'y'}); err != nil {
						t.Fatal(err)
					}
					select {
					case result := <-ready:
						if result.Err != nil || len(result.Fields) != 1 || result.Fields[0].Name != "x" || result.Fields[0].Value != "y" {
							t.Fatal(result)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}

				for {
					critical, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					critical.SetReadDeadline(deadline)
					var prefix [1]byte
					if _, err = io.ReadFull(critical, prefix[:]); err != nil {
						t.Fatal(err)
					}
					if prefix[0] != 3 {
						continue
					}
					var ack [1]byte
					if _, err = io.ReadFull(critical, ack[:]); err != nil {
						t.Fatal(err)
					}
					expected := byte(0x80)
					expectedType := qpack.SectionAcknowledgment
					if canceled {
						expected = 0x40
						expectedType = qpack.StreamCancellation
					}
					if ack[0] != expected|byte(stream.StreamID()) {
						t.Fatalf("ack wire %x", ack)
					}
					instruction, n, err := qpack.ConsumeDecoderInstruction(ack[:])
					if err != nil || n != 1 || instruction.Type != expectedType || instruction.Value != uint64(stream.StreamID()) {
						t.Fatal(instruction, err)
					}
					break
				}
			})
		}
	}
}
