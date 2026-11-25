package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// rawEchoStream is what both implementations' raw streams have in common.
type rawEchoStream interface {
	io.ReadWriteCloser
	SendDatagram([]byte) error
	ReceiveDatagram(context.Context) ([]byte, error)
}

// rawEcho is the server side of these tests' protocol: echo one datagram, echo
// each DATA read, and end the stream when the peer does.
func rawEcho(ctx context.Context, stream rawEchoStream) error {
	payload, err := stream.ReceiveDatagram(ctx)
	if err != nil {
		return fmt.Errorf("receive datagram: %w", err)
	}
	if err := stream.SendDatagram(payload); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	buffer := make([]byte, 4096)
	for {
		n, err := stream.Read(buffer)
		if n > 0 {
			if _, err := stream.Write(buffer[:n]); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
		if err == io.EOF {
			return stream.Close()
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
	}
}

// rawExchange drives the client side of rawEcho after the response arrived.
func rawExchange(ctx context.Context, stream rawEchoStream, tryWriteAll func([]byte) error) error {
	datagram := bytes.Repeat([]byte{0x5a}, 700)
	if err := stream.SendDatagram(datagram); err != nil {
		return fmt.Errorf("send datagram: %w", err)
	}
	if got, err := stream.ReceiveDatagram(ctx); err != nil || !bytes.Equal(got, datagram) {
		return fmt.Errorf("datagram echo %x: %v", got, err)
	}
	for _, chunk := range [][]byte{[]byte("first"), bytes.Repeat([]byte("raw stream "), 300)} {
		if _, err := stream.Write(chunk); err != nil {
			return fmt.Errorf("write: %w", err)
		}
		echo := make([]byte, len(chunk))
		if _, err := io.ReadFull(stream, echo); err != nil || !bytes.Equal(echo, chunk) {
			return fmt.Errorf("echo of %d bytes: %v", len(chunk), err)
		}
	}
	if err := tryWriteAll([]byte("queued at once")); err != nil {
		return fmt.Errorf("TryWriteAll: %w", err)
	}
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	rest, err := io.ReadAll(stream)
	if err != nil || string(rest) != "queued at once" {
		return fmt.Errorf("final echo %q: %v", rest, err)
	}
	return nil
}

// Our raw request stream against upstream's server and its HTTPStreamer, from
// a dialed client and from a raw client connection with its own accept loops.
func TestHTTP3RawRequestStreamIndependentServer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/raw=%t", version, raw), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				handled := make(chan error, 1)
				server := &peerh3.Server{TLSConfig: st, EnableDatagrams: true, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodConnect {
						fmt.Fprintf(w, "%s %s", r.Method, r.URL.Path)
						return
					}
					if r.Proto != "raw-test" {
						handled <- fmt.Errorf("unexpected protocol %q", r.Proto)
						return
					}
					w.WriteHeader(http.StatusOK)
					handled <- rawEcho(ctx, peerHTTPStream(w))
				})}
				socket, err := net.ListenPacket("udp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { cancel(); server.Close(); socket.Close(); <-served }()
				address := socket.LocalAddr().String()
				quicConfig := &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: true}

				var client *ownh3.ClientConn
				if raw {
					conn, err := quic.DialAddr(ctx, address, ct, quicConfig)
					if err != nil {
						t.Fatal(err)
					}
					rawClient := (&ownh3.Transport{EnableDatagrams: true}).NewRawClientConn(conn)
					client = rawClient.ClientConn
					var loop sync.WaitGroup
					loop.Add(1)
					go func() {
						defer loop.Done()
						for {
							stream, err := conn.AcceptUniStream(ctx)
							if err != nil {
								return
							}
							go rawClient.HandleUnidirectionalStream(stream)
						}
					}()
					defer func() { client.Close(); loop.Wait() }()
				} else {
					client, err = ownh3.Dial(ctx, "https://"+address, ct, quicConfig, &ownh3.ClientConfig{EnableDatagrams: true})
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
				}

				stream, err := client.OpenRequestStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request, _ := http.NewRequestWithContext(ctx, http.MethodConnect, "https://"+address+"/raw", nil)
				request.Proto = "raw-test"
				if err := stream.SendRequestHeader(request); err != nil {
					t.Fatal(err)
				}
				response, err := stream.ReadResponse()
				if err != nil || response.StatusCode != http.StatusOK {
					t.Fatal(response, err)
				}
				select {
				case <-client.ReceivedSettings():
				case <-ctx.Done():
					t.Fatal("no SETTINGS")
				}
				if settings := client.Settings(); settings == nil || !settings.EnableDatagrams || !settings.EnableExtendedConnect {
					t.Fatalf("%+v", settings)
				}
				if err := rawExchange(ctx, stream, stream.TryWriteAll); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-handled:
					if err != nil {
						t.Fatal("upstream handler:", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}

				// The same connection still serves an ordinary request on a raw stream.
				plain, err := client.OpenRequestStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "https://"+address+"/plain", nil)
				if err := plain.SendRequestHeader(request); err != nil {
					t.Fatal(err)
				}
				if err := plain.Close(); err != nil {
					t.Fatal(err)
				}
				if response, err = plain.ReadResponse(); err != nil || response.StatusCode != http.StatusOK {
					t.Fatal(response, err)
				}
				if body, err := io.ReadAll(plain); err != nil || string(body) != "GET /plain" {
					t.Fatalf("%q %v", body, err)
				}
			})
		}
	}
}

// Upstream's raw request stream against our server's HTTPStreamer, served from
// a listener and from a raw server connection with its own accept loops. The
// handler returns as soon as it has taken the stream over.
func TestHTTP3HTTPStreamerIndependentClient(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/raw=%t", version, raw), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				handled := make(chan error, 1)
				server := &ownh3.Server{DatagramHandlers: map[string]http.Handler{"raw-test": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					stream, err := w.(ownh3.HTTPStreamer).HTTPStream()
					if err != nil {
						handled <- err
						return
					}
					go func() { handled <- rawEcho(ctx, stream) }()
				})}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintf(w, "%s %s", r.Method, r.URL.Path)
				})}
				var loops sync.WaitGroup
				defer func() { server.Close(); cancel(); loops.Wait() }()
				if raw {
					loops.Add(1)
					go func() {
						defer loops.Done()
						conn, err := listener.Accept(ctx)
						if err != nil {
							return
						}
						serverConn, err := server.NewRawServerConn(conn)
						if err != nil {
							handled <- err
							return
						}
						loops.Add(1)
						go func() {
							defer loops.Done()
							for {
								stream, err := conn.AcceptUniStream(ctx)
								if err != nil {
									return
								}
								go serverConn.HandleUnidirectionalStream(stream)
							}
						}()
						for {
							stream, err := conn.AcceptStream(ctx)
							if err != nil {
								return
							}
							go serverConn.HandleRequestStream(stream)
						}
					}()
				} else {
					loops.Add(1)
					go func() { defer loops.Done(); server.Serve(ctx, listener) }()
				}

				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "done")
				client := (&peerh3.Transport{Dial: loopbackDial(t), EnableDatagrams: true, DisableCompression: true}).NewClientConn(peer)
				stream, err := client.OpenRequestStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request, _ := http.NewRequestWithContext(ctx, http.MethodConnect, "https://"+listener.Addr().String()+"/raw", nil)
				request.Proto = "raw-test"
				if err := stream.SendRequestHeader(request); err != nil {
					t.Fatal(err)
				}
				response, err := stream.ReadResponse()
				if err != nil || response.StatusCode != http.StatusOK {
					t.Fatal(response, err)
				}
				if err := rawExchange(ctx, stream, stream.TryWriteAll); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-handled:
					if err != nil {
						t.Fatal("our handler:", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}

				// The taken-over stream is finished, so the connection serves on.
				request, _ = http.NewRequestWithContext(ctx, http.MethodGet, "https://"+listener.Addr().String()+"/plain", nil)
				response, err = client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "GET /plain" {
					t.Fatalf("%q %v", body, err)
				}
			})
		}
	}
}
