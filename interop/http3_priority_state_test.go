package interop

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	hc "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
)

type priorityObservedStream struct {
	*quic.Stream
	observed chan h3.Priority
}

func (s *priorityObservedStream) SetPriority(u int8, i bool) {
	s.Stream.SetPriority(u, i)
	s.observed <- h3.Priority{Urgency: u, Incremental: i}
}

type priorityObservedAcceptor struct {
	conn     *quic.Conn
	observed chan h3.Priority
}

func (a priorityObservedAcceptor) AcceptRequest(ctx context.Context) (hc.ServerRequestStream, error) {
	s, err := a.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &priorityObservedStream{s, a.observed}, nil
}

// Raw HTTP/3 frames over an independent QUIC peer. The observer forwards each
// scheduling call to the real transport, without changing request parsing.
func TestHTTP3PriorityStateIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			listener, err := quic.ListenAddr("127.0.0.1:0", st, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			conn, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			local, err := hc.OpenLocal(ctx, hc.QUICTransport{Conn: conn}, liveHTTP3Settings())
			if err != nil {
				t.Fatal(err)
			}
			priorities, _ := hc.NewRequestPriorities(4)
			processed := make(chan struct{}, 8)
			session, err := hc.StartSession(ctx, hc.QUICTransport{Conn: conn}, local, hc.SessionConfig{
				LocalSettings: liveHTTP3Settings(), MaxRequests: 4,
				Decoder:          qpack.DecoderLimits{MaxTableCapacity: 4096, MaxBlockedStreams: 8, MaxBufferedBytes: 65536, MaxEncodedSection: 65536, MaxDecodedSection: 65536, MaxFields: 64, MaxInstructionLiteralBytes: 65536},
				Encoder:          hc.EncoderLimits{Capacity: 4096, MaxBlocked: 8, MaxSections: 64, MaxReferences: 256, MaxEncodedSection: 65536, MaxFields: 64, MaxInstruction: 65536, MaxFieldSection: 65536},
				MaxActiveStreams: 8, MaxSettingsBytes: 65536, MaxSettingsEntries: 64, MaxEncoderInstruction: 65536,
				Control: func(_ context.Context, event h3.ControlEvent) error {
					err := priorities.Update(event)
					processed <- struct{}{}
					return err
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			control, err := peer.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = control.Write([]byte{0, 4, 0}); err != nil {
				t.Fatal(err)
			}
			update := func(value string) {
				t.Helper()
				payload := append([]byte{0}, value...)
				frame, e := h3.AppendHeader(nil, h3.Header{Type: h3.PriorityUpdateRequest, Length: uint64(len(payload))})
				if e != nil {
					t.Fatal(e)
				}
				if _, e = control.Write(append(frame, payload...)); e != nil {
					t.Fatal(e)
				}
				select {
				case <-processed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			update("u=0, i") // definitely processed before the request exists
			observed := make(chan h3.Priority, 8)
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- session.ServeRequests(ctx, priorityObservedAcceptor{conn, observed}, hc.ServeRequestsConfig{MaxActive: 4, Request: hc.ServeRequestConfig{Priorities: priorities, MaxEncodedHeaders: 65536, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					close(entered)
					select {
					case <-release:
					case <-r.Context().Done():
					}
					w.WriteHeader(204)
				})}})
			}()
			defer func() {
				cancel()
				conn.CloseWithError(0, "")
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("request loop failed to stop")
				}
			}()
			request, err := peer.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			encoder, _ := qpack.NewEncoderTable(0, 0, 4, 16)
			fields := []qpack.DecodedField{}
			for _, field := range []qpack.Field{{Name: ":method", Value: "GET"}, {Name: ":scheme", Value: "https"}, {Name: ":authority", Value: "localhost"}, {Name: ":path", Value: "/"}, {Name: "priority", Value: "u=7"}} {
				fields = append(fields, qpack.DecodedField{Field: field})
			}
			section, err := encoder.EncodeFields(0, fields, 4096, 4096, 16)
			if err != nil {
				t.Fatal(err)
			}
			frame, _ := h3.AppendHeader(nil, h3.Header{Type: h3.Headers, Length: uint64(len(section))})
			if _, err = request.Write(append(frame, section...)); err != nil {
				t.Fatal(err)
			}
			request.Close()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case got := <-observed:
				if got != (h3.Priority{Urgency: 0, Incremental: true}) {
					t.Fatal(got)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case got := <-observed:
				t.Fatal("header replaced early update", got)
			default:
			}
			update("u=6")
			select {
			case got := <-observed:
				if got != (h3.Priority{Urgency: 6}) {
					t.Fatal(got)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			update("u=1, invalid=")
			select {
			case got := <-observed:
				t.Fatal("malformed update applied", got)
			default:
			}
			close(release)
			request.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := io.ReadAll(request); err != nil {
				t.Fatal(err)
			}
		})
	}
}
