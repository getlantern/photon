package interop

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	h3conn "github.com/getlantern/photon/internal/http3conn"
	"github.com/getlantern/photon/internal/qpack"
)

func TestLSQPACKInteractiveFeedback(t *testing.T) {
	directory := os.Getenv("LSQPACK_BIN")
	if directory == "" {
		t.Skip("set LSQPACK_BIN to the pinned peer build/bin")
	}
	for _, duplicates := range []bool{false, true} {
		for _, cancelSome := range []bool{false, true} {
			for _, fragmented := range []bool{false, true} {
				t.Run(fmt.Sprintf("duplicates=%t/cancel=%t/fragmented=%t", duplicates, cancelSome, fragmented), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					var args []string
					if duplicates {
						args = append(args, "--duplicates")
					}
					cmd := exec.CommandContext(ctx, filepath.Join(directory, "encoder-peer"), args...)
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					input, err := cmd.StdinPipe()
					if err != nil {
						cancel()
						t.Fatal(err)
					}
					output, err := cmd.StdoutPipe()
					if err != nil {
						cancel()
						t.Fatal(err)
					}
					if err := cmd.Start(); err != nil {
						cancel()
						t.Fatal(err)
					}
					waited := false
					t.Cleanup(func() {
						input.Close()
						if !waited {
							cancel()
							cmd.Wait()
						}
						cancel()
					})
					reader := bufio.NewReader(output)
					write := func(data []byte) {
						t.Helper()
						if n, err := input.Write(data); err != nil || n != len(data) {
							t.Fatal("peer input", n, err)
						}
					}
					read32 := func() uint32 {
						t.Helper()
						var n uint32
						if err := binary.Read(reader, binary.BigEndian, &n); err != nil {
							t.Fatal("peer output", err)
						}
						return n
					}
					readBytes := func(n uint32) []byte {
						t.Helper()
						if n > 131072 {
							t.Fatal("oversized peer record", n)
						}
						b := make([]byte, n)
						if _, err := io.ReadFull(reader, b); err != nil {
							t.Fatal(err)
						}
						return b
					}
					var feedback bytes.Buffer
					decoder, err := h3conn.NewFieldDecoder(qpack.DecoderLimits{MaxTableCapacity: 256, MaxBlockedStreams: 8, MaxBufferedBytes: 65536, MaxEncodedSection: 65536, MaxDecodedSection: 65536, MaxFields: 64, MaxInstructionLiteralBytes: 65536}, &feedback)
					if err != nil {
						t.Fatal(err)
					}
					defer decoder.Close(nil)
					feed := func(updates []byte) {
						for len(updates) > 0 {
							_, n, err := qpack.ConsumeEncoderInstruction(updates, 65536)
							if err != nil {
								t.Fatal(err)
							}
							if err := decoder.EncoderInstruction(updates[:n]); err != nil {
								t.Fatal(err)
							}
							updates = updates[n:]
						}
					}
					// Unlike the offline CLI, this adapter transmits ls-qpack's initial capacity.
					feed(readBytes(read32()))
					feedbackBytes, blocked, cancellations := 0, 0, 0
					for i := 0; i < 80; i++ {
						id := uint64(i * 4)
						fields := lsqpackFields(i)
						command := binary.BigEndian.AppendUint64([]byte{'E'}, id)
						command = binary.BigEndian.AppendUint32(command, uint32(len(fields)))
						for _, f := range fields {
							command = binary.BigEndian.AppendUint32(command, uint32(len(f.Name)))
							command = binary.BigEndian.AppendUint32(command, uint32(len(f.Value)))
							command = append(command, f.Name...)
							command = append(command, f.Value...)
						}
						write(command)
						updatesLen, blockLen := read32(), read32()
						updates, block := readBytes(updatesLen), readBytes(blockLen)
						result, err := decoder.Submit(id, block)
						if err != nil {
							t.Fatal(err)
						}
						var decoded h3conn.DecodedSection
						ready := false
						select {
						case decoded = <-result:
							ready = true
						default:
							blocked++
						}
						canceled := cancelSome && !ready && i%3 == 0
						if canceled {
							if err := decoder.Cancel(id); err != nil {
								t.Fatal(err)
							}
							cancellations++
						}
						feed(updates)
						if !ready {
							select {
							case decoded = <-result:
							default:
								t.Fatal("section did not resume", i)
							}
						}
						if canceled {
							if decoded.Err != h3conn.ErrFieldSectionCanceled {
								t.Fatal(decoded)
							}
						} else if decoded.Err != nil || fieldText(decoded.Fields) != fieldText(fields) {
							t.Fatal(i, decoded)
						}
						for feedback.Len() > 0 {
							n := feedback.Len()
							if fragmented {
								n = 1
							}
							data := feedback.Next(n)
							feedbackBytes += len(data)
							command = binary.BigEndian.AppendUint32([]byte{'F'}, uint32(len(data)))
							command = append(command, data...)
							write(command)
							status, err := reader.ReadByte()
							if err != nil || status != 0 {
								t.Fatal("independent encoder rejected raw feedback", i, status, err)
							}
						}
					}
					// With duplication enabled this pin falls back to literals after three
					// blocked sections. Keep testing correctness in that mode; the
					// no-duplication mode must sustain all 80 dynamic sections.
					wantBlocked, wantCanceled := 80, 27
					if duplicates && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
						wantBlocked, wantCanceled = 3, 1
					}
					if !cancelSome {
						wantCanceled = 0
					}
					if blocked != wantBlocked || feedbackBytes == 0 || cancellations != wantCanceled {
						t.Fatal("insufficient coverage", blocked, feedbackBytes, cancellations)
					}
					write([]byte{'Q'})
					input.Close()
					err = cmd.Wait()
					waited = true
					if err != nil {
						t.Fatal(err, stderr.String())
					}
					t.Logf("blocked=%d cancellations=%d feedback bytes=%d", blocked, cancellations, feedbackBytes)
				})
			}
		}
	}
}
