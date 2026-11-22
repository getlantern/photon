package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

type preferredDeclineTrace struct{ advertised chan netip.AddrPort }

func (t *preferredDeclineTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *preferredDeclineTrace) SupportsSchemas(string) bool      { return true }
func (t *preferredDeclineTrace) Close() error                     { return nil }
func (t *preferredDeclineTrace) RecordEvent(event qlogwriter.Event) {
	if e, ok := event.(qlog.ParametersSet); ok && e.PreferredAddress != nil {
		address := e.PreferredAddress.IPv4
		if !address.IsValid() || address.Port() == 0 || address.Addr().IsUnspecified() {
			address = e.PreferredAddress.IPv6
		}
		select {
		case t.advertised <- address:
		default:
		}
	}
}

// Upstream v0.62.0 accepts preferred-address parameters without migrating.
// Verify authenticated advertisement and continued original-address service.
func TestPreferredAddressDeclinedUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				address := netip.AddrPortFrom(netip.MustParseAddr(host), 0)
				listener, err := quic.ListenAddr(address.String(), st, &quic.Config{Versions: []quic.Version{version}, PreferredAddresses: []netip.AddrPort{address}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				trace := &preferredDeclineTrace{advertised: make(chan netip.AddrPort, 4)}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-trace.advertised:
					if got != listener.PreferredAddresses()[0] {
						t.Fatal("advertised address", got)
					}
				case <-ctx.Done():
					t.Fatal("missing preferred parameter", ctx.Err())
				}
				request := bytes.Repeat([]byte("declined-preferred"), 32)
				stream, err := peer.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream.SetDeadline(time.Now().Add(2 * time.Second))
				if _, err := stream.Write(request); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				incoming, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				incoming.SetDeadline(time.Now().Add(2 * time.Second))
				got, err := io.ReadAll(incoming)
				if err != nil || !bytes.Equal(got, request) {
					t.Fatal("request", len(got), err)
				}
				if _, err := incoming.Write(got); err != nil {
					t.Fatal(err)
				}
				if err := incoming.Close(); err != nil {
					t.Fatal(err)
				}
				echoed, err := io.ReadAll(stream)
				if err != nil || !bytes.Equal(echoed, request) {
					t.Fatal("echo", len(echoed), err)
				}
				if peer.RemoteAddr().String() != listener.Addr().String() || server.LocalAddr().String() != listener.Addr().String() {
					t.Fatal("declined migration changed address")
				}
			})
		}
	}
}
