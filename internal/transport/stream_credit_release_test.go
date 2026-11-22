package transport

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamCreditReleaseRequiresCompletionAndConsent(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uni=%t", vName(version), uni), func(t *testing.T) {
				p := streamParameters()
				p.InitialMaxStreamsUni, p.InitialMaxStreamsBidi = 1, 1
				client, server := testPair(t, version, p)
				server.config.RequireStreamCreditRelease = true
				drivePair(t, client, server, false, false)
				if !server.RequireStreamCreditRelease() || client.RequireStreamCreditRelease() {
					t.Fatal("setting not reported per connection")
				}
				limit := func() uint64 { return server.streams.ids.ReceiveLimit(uni) }
				// The client opens, both sides finish, and the server reclaims storage.
				finish := func(consentFirst bool) uint64 {
					t.Helper()
					id, err := client.OpenStream(uni)
					if err != nil {
						t.Fatal("open", err)
					}
					if _, err := client.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("stream limit not enforced", err)
					}
					writeAll(t, client, id, []byte("done"))
					drivePair(t, client, server, false, false)
					if accepted, err := server.AcceptStream(uni); err != nil || accepted != id {
						t.Fatal(accepted, id, err)
					}
					before := limit()
					if consentFirst {
						for range 2 {
							if err := server.ReleaseStreamCredit(id); err != nil {
								t.Fatal(err)
							}
						}
						if limit() != before {
							t.Fatal("credit returned for a live stream")
						}
					}
					if n, err := server.ReadStream(id, make([]byte, 4)); n != 4 || err != io.EOF {
						t.Fatal(n, err)
					}
					if !uni {
						if _, err := server.WriteStream(id, nil, true); err != nil {
							t.Fatal(err)
						}
						drivePair(t, client, server, false, false)
						if _, err := client.ReadStream(id, make([]byte, 1)); err != io.EOF {
							t.Fatal(err)
						}
					}
					if err := server.ReleaseStream(id); err != nil {
						t.Fatal(err)
					}
					if err := client.ReleaseStream(id); err != nil {
						t.Fatal(err)
					}
					if len(server.streams.objects) != 0 {
						t.Fatal("stream storage retained")
					}
					want := before
					if consentFirst {
						want++
					}
					if limit() != want {
						t.Fatal("limit after completion", limit(), want)
					}
					drivePair(t, client, server, false, false)
					return id
				}
				for i := 0; i < 50; i++ {
					id := finish(i%2 == 1)
					if i%2 == 1 {
						if len(server.streams.creditWithheld) != 0 {
							t.Fatal("consented stream left a withheld credit")
						}
						continue
					}
					if _, err := client.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("credit returned without consent", err)
					}
					// Consent for a locally initiated or unknown stream changes nothing.
					before := limit()
					for _, other := range []uint64{id | 1, id + 4, id ^ 2} {
						if err := server.ReleaseStreamCredit(other); err != nil {
							t.Fatal(err)
						}
					}
					if limit() != before || len(server.streams.creditWithheld) != 1 {
						t.Fatal("unrelated consent changed credit", limit(), before)
					}
					for range 3 {
						if err := server.ReleaseStreamCredit(id); err != nil {
							t.Fatal(err)
						}
					}
					if limit() != before+1 || len(server.streams.creditWithheld) != 0 {
						t.Fatal("repeated consent", limit(), before)
					}
					drivePair(t, client, server, false, false)
				}
				// Without the setting, release alone returns credit and consent is inert.
				before := client.streams.ids.ReceiveLimit(true)
				if !uni {
					return
				}
				id, err := server.OpenStream(true)
				if err != nil {
					t.Fatal(err)
				}
				writeAll(t, server, id, []byte("done"))
				drivePair(t, client, server, false, false)
				if _, err := client.AcceptStream(true); err != nil {
					t.Fatal(err)
				}
				if n, err := client.ReadStream(id, make([]byte, 4)); n != 4 || err != io.EOF {
					t.Fatal(n, err)
				}
				if err := client.ReleaseStream(id); err != nil {
					t.Fatal(err)
				}
				if err := client.ReleaseStreamCredit(id); err != nil {
					t.Fatal(err)
				}
				if got := client.streams.ids.ReceiveLimit(true); got != before+1 || len(client.streams.creditWithheld) != 0 {
					t.Fatal("default release", got, before)
				}
			})
		}
	}
}
