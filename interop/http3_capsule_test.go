package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"io"
	"net"
	"net/http"
	"testing"
	"testing/iotest"
	"time"

	own "github.com/getlantern/photon/http3"
	peer "github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

// This checks independent codec interoperability, not HTTP negotiation or resets.
func TestCapsuleCodecInterop(t *testing.T) {
	types := []uint64{0, 2, 6, 8, 9, 63, 64, 16383, 16384, 1 << 30, 1<<62 - 1}
	payloads := [][]byte{nil, []byte("capsule"), bytes.Repeat([]byte{0x80, 0, 0xff}, 23000)}
	var ours, theirs bytes.Buffer
	for _, typ := range types {
		for _, payload := range payloads {
			if err := own.WriteCapsule(&ours, own.CapsuleType(typ), payload); err != nil {
				t.Fatal(err)
			}
			if err := peer.WriteCapsule(&theirs, peer.CapsuleType(typ), payload); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !bytes.Equal(ours.Bytes(), theirs.Bytes()) {
		t.Fatal("different wire encoding")
	}
	a := own.NewCapsuleParser(iotest.OneByteReader(&theirs))
	b := peer.NewCapsuleParser(iotest.OneByteReader(&ours))
	for _, typ := range types {
		for _, payload := range payloads {
			at, ar, err := a.Next()
			if err != nil || uint64(at) != typ {
				t.Fatal(at, err)
			}
			bt, br, err := b.Next()
			if err != nil || uint64(bt) != typ {
				t.Fatal(bt, err)
			}
			av, err := io.ReadAll(ar)
			if err != nil || !bytes.Equal(av, payload) {
				t.Fatal("own decode", err)
			}
			bv, err := io.ReadAll(br)
			if err != nil || !bytes.Equal(bv, payload) {
				t.Fatal("peer decode", err)
			}
		}
	}
	if _, _, err := a.Next(); err != io.EOF {
		t.Fatal(err)
	}
	if _, _, err := b.Next(); err != io.EOF {
		t.Fatal(err)
	}
}

// Extension handshake policy is explicit in this fixture. This exercises capsule
// streaming over HTTP DATA and FIN; automatic endpoint capsule policy is separate.
func TestCapsuleTunnelIndependentServer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			server := &peer.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "CONNECT" || r.Proto != "capsule-test" || r.Header.Get("Capsule-Protocol") != "?1" {
					done <- fmt.Errorf("invalid capsule handshake")
					w.WriteHeader(400)
					return
				}
				w.Header().Set("Capsule-Protocol", "?1")
				w.WriteHeader(200)
				raw := peerHTTPStream(w)
				parser := peer.NewCapsuleParser(r.Body)
				for {
					typ, payload, err := parser.Next()
					if err == io.EOF {
						done <- raw.Close()
						return
					}
					if err != nil {
						done <- err
						return
					}
					if typ != 0 {
						if err := payload.Discard(); err != nil {
							done <- err
							return
						}
						continue
					}
					value, err := io.ReadAll(payload)
					if err != nil {
						done <- err
						return
					}
					if err := peer.WriteCapsule(quicvarint.NewWriter(raw), 0, value); err != nil {
						done <- err
						return
					}
				}
			})}
			socket, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- server.Serve(socket) }()
			defer func() { server.Close(); socket.Close(); <-served }()
			origin := "https://" + socket.LocalAddr().String()
			client, err := own.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, &own.ClientConfig{CapsuleProtocols: map[string]bool{"capsule-test": true}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/capsules", nil)
			request.Header.Set("Capsule-Protocol", "?1")
			tunnel, response, err := client.OpenExtendedTunnel(request, "capsule-test")
			if err != nil {
				t.Fatal(err)
			}
			if tunnel == nil {
				response.Body.Close()
				t.Fatal(response.StatusCode)
			}
			defer tunnel.Close()
			if !own.CapsuleProtocol(response.Header) {
				t.Fatal("missing capsule acknowledgement")
			}
			parser := own.NewCapsuleParser(tunnel)
			for _, value := range [][]byte{nil, []byte("hello"), bytes.Repeat([]byte("capsule"), 1000)} {
				if err := own.WriteCapsule(tunnel, 6, []byte("unknown extension data")); err != nil {
					t.Fatal(err)
				}
				if err := own.WriteCapsule(tunnel, 0, value); err != nil {
					t.Fatal(err)
				}
				typ, payload, err := parser.Next()
				if err != nil || typ != 0 {
					t.Fatal(typ, err)
				}
				got, err := io.ReadAll(payload)
				if err != nil || !bytes.Equal(got, value) {
					t.Fatal("echo mismatch", len(got), err)
				}
			}
			if err := tunnel.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := parser.Next(); err != io.EOF {
				t.Fatal("unclean capsule FIN", err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestCapsuleTunnelIndependentClient(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			results := make(chan error, 4)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !own.CapsuleProtocol(r.Header) {
					w.WriteHeader(400)
					return
				}
				w.Header().Set("Capsule-Protocol", "?1")
				tunnel, err := own.AcceptTunnel(w)
				if err != nil {
					results <- err
					return
				}
				parser := own.NewCapsuleParser(tunnel)
				for {
					typ, payload, err := parser.Next()
					if err == nil && typ != 0 {
						err = payload.Discard()
						if err == nil {
							continue
						}
					}
					if err == nil {
						var value []byte
						value, err = io.ReadAll(payload)
						if err == nil {
							err = own.WriteCapsule(tunnel, typ, value)
						}
					}
					if err == nil {
						continue
					}
					var malformed *own.CapsuleError
					if errors.As(err, &malformed) {
						// Parser automatically aborts the ServerTunnel with request code 0x10e.
						results <- err
						return
					}
					if err == io.EOF {
						err = nil
					}
					results <- err
					return
				}
			})
			server := &own.Server{CapsuleProtocols: map[string]bool{"capsule-test": true}, MaxConcurrentRequests: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "healthy") }), ExtendedConnectHandlers: map[string]http.Handler{"capsule-test": handler}}
			served := make(chan error, 1)
			go func() { served <- server.Serve(ctx, listener) }()
			defer func() { server.Close(); cancel(); <-served }()
			conn, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseWithError(0, "done")
			transport := &peer.Transport{}
			client := transport.NewClientConn(conn)
			origin := "https://" + listener.Addr().String()
			for _, malformed := range []bool{false, true} {
				stream, err := client.OpenRequestStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin+"/capsules", nil)
				request.Proto = "capsule-test"
				request.Header.Set("Capsule-Protocol", "?1")
				if err := stream.SendRequestHeader(request); err != nil {
					t.Fatal(err)
				}
				response, err := stream.ReadResponse()
				if err != nil || response.StatusCode != 200 {
					t.Fatal(response, err)
				}
				if malformed {
					// A valid capsule header declares three bytes, but the peer sends only one.
					if _, err := stream.Write([]byte{0, 3, 'x'}); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					_, err = io.ReadAll(stream)
					var reset *peer.Error
					if !errors.As(err, &reset) || reset.ErrorCode != 0x10e || !reset.Remote {
						t.Fatalf("wrong request error: %T %v", err, err)
					}
				} else {
					parser := peer.NewCapsuleParser(stream)
					for _, value := range [][]byte{nil, []byte("hello"), bytes.Repeat([]byte("capsule"), 1000)} {
						if err := peer.WriteCapsule(quicvarint.NewWriter(stream), 6, []byte("skip")); err != nil {
							t.Fatal(err)
						}
						if err := peer.WriteCapsule(quicvarint.NewWriter(stream), 0, value); err != nil {
							t.Fatal(err)
						}
						typ, payload, err := parser.Next()
						if err != nil || typ != 0 {
							t.Fatal(typ, err)
						}
						got, err := io.ReadAll(payload)
						if err != nil || !bytes.Equal(got, value) {
							t.Fatal("echo", len(got), err)
						}
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					if _, _, err := parser.Next(); err != io.EOF {
						t.Fatal(err)
					}
				}
				select {
				case err := <-results:
					var capsule *own.CapsuleError
					if malformed {
						if !errors.As(err, &capsule) {
							t.Fatal(err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			// The malformed capsule must release the sole request slot and preserve QUIC.
			stream, err := client.OpenRequestStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/healthy", nil)
			if err := stream.SendRequestHeader(request); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			response, err := stream.ReadResponse()
			if err != nil || response.StatusCode != 200 {
				t.Fatal(response, err)
			}
			body, err := io.ReadAll(stream)
			if err != nil || string(body) != "healthy" {
				t.Fatal(string(body), err)
			}
		})
	}
}
