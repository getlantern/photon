package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	h3conn "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
	"io"
	"testing"
	"time"
)

// The peer supplies independent QUIC transport; HTTP3 control framing is fixture-owned.
func TestHTTP3LiveGoaway(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
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

			events := make(chan h3.ControlEvent, 4)
			session, err := h3conn.StartSession(ctx, h3conn.QUICTransport{Conn: conn}, local, h3conn.SessionConfig{
				PeerIsServer: false, LocalSettings: liveHTTP3Settings(),
				Decoder:          qpack.DecoderLimits{MaxTableCapacity: 4096, MaxBlockedStreams: 8, MaxBufferedBytes: 65536, MaxEncodedSection: 65536, MaxDecodedSection: 65536, MaxFields: 64, MaxInstructionLiteralBytes: 65536},
				Encoder:          h3conn.EncoderLimits{Capacity: 4096, MaxBlocked: 8, MaxSections: 64, MaxReferences: 256, MaxEncodedSection: 65536, MaxFields: 64, MaxInstruction: 65536, MaxFieldSection: 65536},
				MaxActiveStreams: 8, MaxSettingsBytes: 65536, MaxSettingsEntries: 64, MaxEncoderInstruction: 65536,
				Control: func(ctx context.Context, event h3.ControlEvent) error {
					select {
					case events <- event:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			if _, err = session.WaitEncoder(ctx); err != nil {
				t.Fatal(err)
			}
			var incoming io.Reader
			for incoming == nil {
				stream, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream.SetReadDeadline(deadline)
				var typ [1]byte
				if _, err = io.ReadFull(stream, typ[:]); err != nil {
					t.Fatal(err)
				}
				if typ[0] == 0 {
					incoming = stream
				}
			}
			frames := h3.NewReader(incoming)
			frame, err := frames.Next()
			if err != nil || frame.Type != h3.Settings {
				t.Fatal(frame, err)
			}
			if _, err = io.Copy(io.Discard, frames); err != nil {
				t.Fatal(err)
			}
			for _, id := range []uint64{wire.MaxVarint - 3, 8, 8, 0} {
				if err := session.SendGoaway(id); err != nil {
					t.Fatal(err)
				}
				frame, err = frames.Next()
				if err != nil || frame.Type != h3.Goaway || frame.Length > 8 {
					t.Fatal(frame, err)
				}
				payload, err := io.ReadAll(frames)
				if err != nil {
					t.Fatal(err)
				}
				received, err := h3.ParseID(payload)
				if err != nil || received != id {
					t.Fatal(received, err)
				}
			}
			// Client GOAWAY carries push IDs, so a non-multiple-of-four is legal.
			for _, id := range []byte{5, 5, 0} {
				if _, err := control.Write([]byte{7, 1, id}); err != nil {
					t.Fatal(err)
				}
				select {
				case event := <-events:
					if event.Type != h3.Goaway || event.ID != uint64(id) {
						t.Fatal(event)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				snapshot, ok := session.Goaway()
				if !ok || snapshot.RequestIDs || snapshot.ID != uint64(id) {
					t.Fatal(snapshot, ok)
				}
			}
			// Increasing the peer's limit must terminate the connection, even though
			// the local endpoint is already draining in the other direction.
			if _, err := control.Write([]byte{7, 1, 1}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var application *upstream.ApplicationError
			if !errors.As(context.Cause(peer.Context()), &application) || uint64(application.ErrorCode) != h3.IDError || !application.Remote {
				t.Fatal(context.Cause(peer.Context()))
			}
			select {
			case <-session.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestHTTP3LiveRequestGoaway(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			clientTLS, serverTLS := tlsConfigs(t)
			clientTLS.NextProtos = []string{"h3"}
			serverTLS.NextProtos = []string{"h3"}
			listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			conn, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseWithError(0, "")
			peer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
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
			session := startHTTP3Session(t, ctx, conn, true, local, liveHTTP3Settings())
			encoder, err := session.WaitEncoder(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var streams []*quic.Stream
			var responses []*upstream.Stream
			var leases []*h3conn.RequestLease
			for i := 0; i < 2; i++ {
				stream, err := conn.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream.SetReadDeadline(deadline)
				stream.SetWriteDeadline(deadline)
				lease, err := session.BeginRequest(uint64(stream.StreamID()))
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Finish()
				writer, err := h3conn.NewMessageWriter(stream, encoder, uint64(stream.StreamID()), h3conn.MessageConfig{Kind: h3conn.RequestFields}, nil)
				if err != nil {
					t.Fatal(err)
				}
				fields := []qpack.DecodedField{{Field: qpack.Field{Name: ":method", Value: "GET"}}, {Field: qpack.Field{Name: ":scheme", Value: "https"}}, {Field: qpack.Field{Name: ":authority", Value: "localhost"}}, {Field: qpack.Field{Name: ":path", Value: "/"}}}
				if err = writer.Headers(fields); err != nil {
					t.Fatal(err)
				}
				if err = writer.Close(); err != nil {
					t.Fatal(err)
				}
				response, err := peer.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				response.SetReadDeadline(deadline)
				response.SetWriteDeadline(deadline)
				if _, err = io.ReadAll(response); err != nil {
					t.Fatal(err)
				}
				streams = append(streams, stream)
				responses = append(responses, response)
				leases = append(leases, lease)
			}
			if streams[0].StreamID() != 0 || streams[1].StreamID() != 4 {
				t.Fatal("fixture IDs")
			}
			if _, err = control.Write([]byte{7, 1, 4}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-leases[1].Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if context.Cause(leases[1].Context()) != h3conn.ErrRequestRejected || leases[0].Context().Err() != nil {
				t.Fatal("incorrect GOAWAY boundary")
			}
			if _, err = session.BeginRequest(8); err != h3conn.ErrPeerDraining {
				t.Fatal(err)
			}
			// Endpoint-owned cleanup: the lease signals rejection; it does not reset QUIC.
			owner, err := h3conn.NewFieldStream(session.Decoder(), 4)
			if err != nil {
				t.Fatal(err)
			}
			if err = owner.Cancel(); err != nil {
				t.Fatal(err)
			}
			streams[1].CancelRead(0x10c)
			select {
			case <-responses[1].Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var streamErr *upstream.StreamError
			if !errors.As(context.Cause(responses[1].Context()), &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 0x10c {
				t.Fatal(context.Cause(responses[1].Context()))
			}
			leases[1].Finish()
			// Static QPACK :status=200, followed by DATA "ok" and FIN for accepted stream 0.
			if _, err = responses[0].Write([]byte{1, 3, 0, 0, 0xd9, 0, 2, 'o', 'k'}); err != nil {
				t.Fatal(err)
			}
			if err = responses[0].Close(); err != nil {
				t.Fatal(err)
			}
			owner, err = h3conn.NewFieldStream(session.Decoder(), 0)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := h3conn.NewMessageReader(streams[0], owner, h3conn.MessageConfig{Kind: h3conn.ResponseFields, RequestMethod: "GET"}, 65536)
			if err != nil {
				t.Fatal(err)
			}
			event, err := reader.Next(leases[0].Context())
			if err != nil || event.Control.Status != 200 {
				t.Fatal(event, err)
			}
			completed := false
			bodyReader, err := h3conn.NewMessageBody(leases[0].Context(), reader, h3conn.BodyConfig{Close: func(complete bool) error {
				completed = complete
				defer leases[0].Finish()
				if !complete {
					streams[0].CancelRead(0x10c)
					return owner.Cancel()
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer bodyReader.Close()
			body, err := io.ReadAll(bodyReader)
			if err != nil || string(body) != "ok" {
				t.Fatal(string(body), err)
			}
			if leases[0].Context().Err() != nil {
				t.Fatal("accepted request canceled")
			}
			if err = bodyReader.Close(); err != nil || !completed {
				t.Fatal("body cleanup", err, completed)
			}

		})
	}
}
