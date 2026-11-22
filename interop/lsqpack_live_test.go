package interop

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3conn "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
)

// Independent QPACK encoder + independent QUIC transport, with test-owned HTTP3
// framing. This is not a complete independent HTTP3 endpoint interoperability test.
func TestLSQPACKLive(t *testing.T) {
	directory := os.Getenv("LSQPACK_BIN")
	if directory == "" {
		t.Skip("set LSQPACK_BIN to the pinned peer build/bin")
	}
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, cancelSome := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/cancel=%t", uint32(version), cancelSome), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				deadline, _ := ctx.Deadline()
				cmd := exec.CommandContext(ctx, filepath.Join(directory, "encoder-peer"))
				input, err := cmd.StdinPipe()
				if err != nil {
					t.Fatal(err)
				}
				output, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				waited := false
				defer func() {
					input.Close()
					if !waited {
						cancel()
						cmd.Wait()
					}
				}()
				write := func(w io.Writer, b []byte) {
					t.Helper()
					if n, e := w.Write(b); e != nil || n != len(b) {
						t.Fatal("write", n, e)
					}
				}
				read := func(n uint32) []byte {
					t.Helper()
					if n > 131072 {
						t.Fatal("oversized peer record")
					}
					b := make([]byte, n)
					if _, e := io.ReadFull(output, b); e != nil {
						t.Fatal(e)
					}
					return b
				}
				read32 := func() uint32 { return binary.BigEndian.Uint32(read(4)) }
				initial := read(read32())
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
				settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 256}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: 65536}}
				local, err := h3conn.OpenLocal(ctx, h3conn.QUICTransport{Conn: conn}, settings)
				if err != nil {
					t.Fatal(err)
				}
				control, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				control.SetWriteDeadline(deadline)
				write(control, []byte{0, 4, 0})
				_, encoder, decoder := startHTTP3DispatchSettings(t, ctx, conn, true, local, settings)
				updatesStream, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				updatesStream.SetWriteDeadline(deadline)
				write(updatesStream, append([]byte{2}, initial...))
				var feedbackStream io.Reader
				for feedbackStream == nil {
					s, e := peer.AcceptUniStream(ctx)
					if e != nil {
						t.Fatal(e)
					}
					s.SetReadDeadline(deadline)
					var typ [1]byte
					if _, e = io.ReadFull(s, typ[:]); e != nil {
						t.Fatal(e)
					}
					if typ[0] == 3 {
						feedbackStream = s
					}
				}
				var rawFeedback bytes.Buffer
				feedbackReader, err := qpack.NewDecoderInstructionReader(io.TeeReader(feedbackStream, &rawFeedback))
				if err != nil {
					t.Fatal(err)
				}
				var inserted, known uint64
				cancellations := 0
				for i := 0; i < 8; i++ {
					stream, e := conn.OpenStreamSync(ctx)
					if e != nil {
						t.Fatal(e)
					}
					stream.SetReadDeadline(deadline)
					stream.SetWriteDeadline(deadline)
					id := uint64(stream.StreamID())
					request := []qpack.DecodedField{{Field: qpack.Field{Name: ":method", Value: "GET"}}, {Field: qpack.Field{Name: ":scheme", Value: "https"}}, {Field: qpack.Field{Name: ":authority", Value: "localhost"}}, {Field: qpack.Field{Name: ":path", Value: "/"}}}
					requestBlock, e := encoder.Encode(id, request)
					if e != nil {
						t.Fatal(e)
					}
					frame, e := h3.AppendHeader(nil, h3.Header{Type: h3.Headers, Length: uint64(len(requestBlock))})
					if e != nil {
						t.Fatal(e)
					}
					write(stream, append(frame, requestBlock...))
					if e = stream.Close(); e != nil {
						t.Fatal(e)
					}
					response, e := peer.AcceptStream(ctx)
					if e != nil {
						t.Fatal(e)
					}
					response.SetWriteDeadline(deadline)
					response.SetReadDeadline(deadline)
					if uint64(response.StreamID()) != id {
						t.Fatal("stream identity")
					}
					if _, e = io.ReadAll(response); e != nil {
						t.Fatal(e)
					}
					fields := lsqpackFields(i)
					command := binary.BigEndian.AppendUint64([]byte{'E'}, id)
					command = binary.BigEndian.AppendUint32(command, uint32(len(fields)))
					for _, f := range fields {
						command = binary.BigEndian.AppendUint32(command, uint32(len(f.Name)))
						command = binary.BigEndian.AppendUint32(command, uint32(len(f.Value)))
						command = append(command, f.Name...)
						command = append(command, f.Value...)
					}
					write(input, command)
					un, bn := read32(), read32()
					updates, block := read(un), read(bn)
					prefix, _, e := qpack.ConsumeSectionPrefix(block, 256, inserted)
					if e != nil {
						t.Fatal(e)
					}
					for remaining := updates; len(remaining) > 0; {
						instruction, n, e := qpack.ConsumeEncoderInstruction(remaining, 65536)
						if e != nil {
							t.Fatal(e)
						}
						if instruction.Type != qpack.SetDynamicCapacity {
							inserted++
						}
						remaining = remaining[n:]
					}
					frame, e = h3.AppendHeader(nil, h3.Header{Type: h3.Headers, Length: uint64(len(block))})
					if e != nil {
						t.Fatal(e)
					}
					write(response, append(frame, block...))
					frames := h3.NewReader(stream)
					header, e := frames.Next()
					if e != nil || header.Type != h3.Headers {
						t.Fatal(header, e)
					}
					section, e := io.ReadAll(frames)
					if e != nil {
						t.Fatal(e)
					}
					ready, e := decoder.Submit(id, section)
					if e != nil {
						t.Fatal(e)
					}
					select {
					case <-ready:
						t.Fatal("section did not block before encoder updates")
					default:
					}
					canceled := cancelSome && i%3 == 0
					if canceled {
						owner, err := h3conn.NewFieldStream(decoder, id)
						if err != nil {
							t.Fatal(err)
						}
						// Submit directly above establishes blocking without timing sleeps.
						// Exercise the stream owner's idempotent cancellation afterward.
						if err := owner.Cancel(); err != nil {
							t.Fatal(err)
						}
						if err := owner.Cancel(); err != nil {
							t.Fatal(err)
						}
						if _, err := owner.Decode(ctx, section); err != h3conn.ErrFieldSectionCanceled {
							t.Fatal(err)
						}
						stream.CancelRead(quic.StreamErrorCode(0x10c)) // H3_REQUEST_CANCELLED
						select {
						case <-response.Context().Done():
							var streamErr *upstream.StreamError
							if !errors.As(context.Cause(response.Context()), &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 0x10c {
								t.Fatal("peer did not observe request cancellation", context.Cause(response.Context()))
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						cancellations++
					}
					write(updatesStream, updates)
					var decoded h3conn.DecodedSection
					select {
					case decoded = <-ready:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if canceled {
						if decoded.Err != h3conn.ErrFieldSectionCanceled {
							t.Fatal(decoded)
						}
					} else {
						if decoded.Err != nil || fieldText(decoded.Fields) != fieldText(fields) {
							t.Fatal(decoded)
						}
						message, e := h3conn.NewMessage(h3conn.MessageConfig{Kind: h3conn.ResponseFields, RequestMethod: "GET"})
						if e != nil {
							t.Fatal(e)
						}
						if _, _, e = message.Headers(decoded.Fields); e != nil {
							t.Fatal(e)
						}
						if e = response.Close(); e != nil {
							t.Fatal(e)
						}
						if _, e = frames.Next(); e != io.EOF {
							t.Fatal("response FIN", e)
						}
						if e = message.Finish(); e != nil {
							t.Fatal(e)
						}
					}
					acknowledged := false
					for !acknowledged || known < inserted {
						instruction, e := feedbackReader.Next()
						if e != nil {
							t.Fatal(e)
						}
						switch instruction.Type {
						case qpack.SectionAcknowledgment:
							if canceled || instruction.Value != id || acknowledged {
								t.Fatal(instruction)
							}
							acknowledged = true
							if prefix.RequiredInsertCount > known {
								known = prefix.RequiredInsertCount
							}
						case qpack.StreamCancellation:
							if !canceled || instruction.Value != id || acknowledged {
								t.Fatal(instruction)
							}
							acknowledged = true
						case qpack.InsertCountIncrement:
							known += instruction.Value
						default:
							t.Fatal(instruction)
						}
						if known > inserted {
							t.Fatal("feedback exceeds inserts")
						}
					}
					command = binary.BigEndian.AppendUint32([]byte{'F'}, uint32(rawFeedback.Len()))
					command = append(command, rawFeedback.Bytes()...)
					write(input, command)
					rawFeedback.Reset()
					if status := read(1)[0]; status != 0 {
						t.Fatal("encoder rejected live feedback", status)
					}
				}
				write(input, []byte{'Q'})
				input.Close()
				err = cmd.Wait()
				waited = true
				if err != nil {
					t.Fatal(err, stderr.String())
				}
				if cancelSome && cancellations != 3 {
					t.Fatal("cancellation coverage", cancellations)
				}
				t.Logf("8 blocked response sections; %d cancellations; %d insertions acknowledged over QUIC", cancellations, inserted)
			})
		}
	}
}
