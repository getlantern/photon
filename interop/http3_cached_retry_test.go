package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
)

func TestHTTP3CachedOnlyGoawayRetry(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			st.NextProtos = []string{"h3"}
			listener, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			result := make(chan error, 1)
			go func() {
				peer, err := listener.Accept(ctx)
				if err != nil {
					result <- err
					return
				}
				defer peer.CloseWithError(0, "done")
				control, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					result <- err
					return
				}
				if _, err = control.Write([]byte{0, 4, 0}); err != nil {
					result <- err
					return
				}
				warm, err := peer.AcceptStream(ctx)
				if err != nil {
					result <- err
					return
				}
				if warm.StreamID() != 0 {
					result <- fmt.Errorf("warm ID %d", warm.StreamID())
					return
				}
				// Static QPACK status 200 followed by DATA "ok" and FIN.
				if _, err = warm.Write([]byte{1, 3, 0, 0, 0xd9, 0, 2, 'o', 'k'}); err != nil {
					result <- err
					return
				}
				if err = warm.Close(); err != nil {
					result <- err
					return
				}
				next, err := peer.AcceptStream(ctx)
				if err != nil {
					result <- err
					return
				}
				if next.StreamID() != 4 {
					result <- fmt.Errorf("cached request ID %d", next.StreamID())
					return
				}
				// Reject only the second request. The automatic retry must honor the
				// original cached-only option after retiring this connection.
				if _, err = control.Write([]byte{7, 1, 4}); err != nil {
					result <- err
					return
				}
				select {
				case <-peer.Context().Done():
					result <- nil
				case <-ctx.Done():
					result <- ctx.Err()
				}
			}()
			transport := &ownh3.Transport{TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{v}}}
			defer transport.Close()
			origin := "https://" + listener.Addr().String()
			request, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "ok" {
				t.Fatal(string(body), err)
			}
			request, _ = http.NewRequestWithContext(ctx, "GET", origin, nil)
			response, err = transport.RoundTripOpt(request, ownh3.RoundTripOpt{OnlyCachedConn: true})
			if response != nil || !errors.Is(err, ownh3.ErrNoCachedConn) {
				t.Fatal("retry dialed or returned wrong error", response, err)
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
