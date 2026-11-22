package interop

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	h3conn "github.com/getlantern/photon/internal/http3conn"
	"github.com/getlantern/photon/internal/qpack"
)

func lsqpackRun(t *testing.T, name string, input []byte, args ...string) []byte {
	t.Helper()
	directory := os.Getenv("LSQPACK_BIN")
	if directory == "" {
		t.Skip("set LSQPACK_BIN to pinned ls-qpack build/bin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, filepath.Join(directory, name), args...)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s: %v: %s", name, err, stderr.String())
	}
	return out
}
func lsqpackFields(i int) []qpack.DecodedField {
	return []qpack.DecodedField{{Field: qpack.Field{Name: ":status", Value: "200"}}, {Field: qpack.Field{Name: "x-dynamic", Value: fmt.Sprintf("value-%03d", i)}}, {Field: qpack.Field{Name: "x-repeat", Value: "persistent-value"}}}
}
func fieldText(fields []qpack.DecodedField) string {
	var b strings.Builder
	for _, f := range fields {
		fmt.Fprintf(&b, "%s\t%s\n", f.Name, f.Value)
	}
	return b.String()
}
func appendQPACKRecord(dst []byte, id uint64, data []byte) []byte {
	dst = binary.BigEndian.AppendUint64(dst, id)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(data)))
	return append(dst, data...)
}

func TestLSQPACKEncoderToIndependentDecoder(t *testing.T) {
	var input strings.Builder
	for i := 0; i < 80; i++ {
		input.WriteString(fieldText(lsqpackFields(i)))
		input.WriteByte('\n')
	}
	records := lsqpackRun(t, "interop-encode", []byte(input.String()), "-t", "256", "-s", "8", "-a", "1", "-A")
	table, _ := qpack.NewDecoderTable(256)
	// The pinned CLI discards the initial capacity instruction returned by
	// lsqpack_enc_init. Its offline format assumes the -t capacity out of band;
	// mirror that fixture contract without relaxing the production decoder.
	if err := table.SetCapacity(256); err != nil {
		t.Fatal(err)
	}
	sections, dynamic := 0, 0
	for len(records) > 0 {
		if len(records) < 12 {
			t.Fatal("truncated peer record")
		}
		id := binary.BigEndian.Uint64(records)
		n := int(binary.BigEndian.Uint32(records[8:]))
		records = records[12:]
		if n > len(records) {
			t.Fatal("truncated payload")
		}
		body := records[:n]
		records = records[n:]
		if id == 0 {
			for len(body) > 0 {
				consumed, err := table.ConsumeEncoderUpdate(body, 65536)
				if err != nil {
					t.Fatal(err)
				}
				body = body[consumed:]
			}
			continue
		}
		prefix, fields, err := table.DecodeSection(body, 65536, 64)
		if err != nil {
			t.Fatalf("stream %d: %v", id, err)
		}
		if id != uint64(sections+1) || fieldText(fields) != fieldText(lsqpackFields(sections)) {
			t.Fatalf("stream %d fields %+v", id, fields)
		}
		if prefix.RequiredInsertCount > 0 {
			dynamic++
		}
		sections++
	}
	if sections != 80 || dynamic < 40 || table.InsertCount() < 32 {
		t.Fatalf("insufficient dynamic coverage: sections=%d dynamic=%d inserts=%d", sections, dynamic, table.InsertCount())
	}
	t.Logf("sections=%d dynamic=%d inserts=%d", sections, dynamic, table.InsertCount())
}
func TestIndependentEncoderToLSQPACKDecoder(t *testing.T) {
	// The peer's maximum is 256, while our selected table capacity is only 128.
	encoder, _ := qpack.NewEncoderTable(256, 8, 128, 512)
	update, err := encoder.Apply(qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
	if err != nil {
		t.Fatal(err)
	}
	records := appendQPACKRecord(nil, 0, update)
	var expected strings.Builder
	for i := 0; i < 80; i++ {
		fields := lsqpackFields(i)
		update, inserted, err := encoder.InsertField(fields[1], 65536)
		if err != nil || !inserted {
			t.Fatal(i, inserted, err)
		}
		records = appendQPACKRecord(records, 0, update)
		block, err := encoder.EncodeFields(uint64(i+1), fields, 65536, 65536, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(block) < 2 || block[0] == 0 {
			t.Fatal("section did not reference dynamic table", i)
		}
		records = appendQPACKRecord(records, uint64(i+1), block)
		expected.WriteString(fieldText(fields))
		// Offline fixture models prompt acknowledgment, not a live feedback exchange.
		if err := encoder.ApplyFeedback(qpack.DecoderInstruction{Type: qpack.SectionAcknowledgment, Value: uint64(i + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, ordered := range []bool{true, false} {
		t.Run(fmt.Sprintf("ordered=%t", ordered), func(t *testing.T) {
			args := []string{"-t", "256", "-s", "8", "-m", "1"}
			if ordered {
				args = append(args, "-S")
			}
			output := lsqpackRun(t, "interop-decode", records, args...)
			var got strings.Builder
			for _, line := range strings.Split(string(output), "\n") {
				if line != "" && !strings.HasPrefix(line, "#") {
					got.WriteString(line)
					got.WriteByte('\n')
				}
			}
			if got.String() != expected.String() {
				t.Fatalf("peer output mismatch:\n%s", output)
			}
		})
	}
}

// This uses independent encoder bytes with deliberately delayed updates. Feedback
// is checked locally; the offline peer is not consuming it interactively.
func TestLSQPACKDelayedUpdates(t *testing.T) {
	var input strings.Builder
	for i := 0; i < 80; i++ {
		input.WriteString(fieldText(lsqpackFields(i)))
		input.WriteByte('\n')
	}
	encoded := lsqpackRun(t, "interop-encode", []byte(input.String()), "-t", "256", "-s", "8", "-a", "1", "-A")
	for _, cancelSome := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelSome), func(t *testing.T) {
			var feedback bytes.Buffer
			decoder, err := h3conn.NewFieldDecoder(qpack.DecoderLimits{MaxTableCapacity: 256, MaxBlockedStreams: 8, MaxBufferedBytes: 65536, MaxEncodedSection: 65536, MaxDecodedSection: 65536, MaxFields: 64, MaxInstructionLiteralBytes: 65536}, &feedback)
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close(nil)
			// Adapt the pinned CLI's out-of-band initial capacity to an explicit update.
			capacity, _ := qpack.AppendEncoderInstruction(nil, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 256})
			if err := decoder.EncoderInstruction(capacity); err != nil {
				t.Fatal(err)
			}
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
			records := encoded
			var delayed []byte
			sections, blocked, canceled := 0, 0, 0
			expected := make(map[uint64]qpack.DecoderInstructionType)
			for len(records) > 0 {
				if len(records) < 12 {
					t.Fatal("truncated record")
				}
				id := binary.BigEndian.Uint64(records)
				size := int(binary.BigEndian.Uint32(records[8:]))
				records = records[12:]
				if size > len(records) {
					t.Fatal("truncated payload")
				}
				body := records[:size]
				records = records[size:]
				if id == 0 {
					delayed = append(delayed, body...)
					continue
				}
				if id != uint64(sections+1) {
					t.Fatal("unexpected peer stream", id)
				}
				result, err := decoder.Submit(id, body)
				if err != nil {
					t.Fatal(err)
				}
				var decoded h3conn.DecodedSection
				wasReady := false
				wasCanceled := false
				select {
				case decoded = <-result:
					wasReady = true
				default:
					blocked++
				}
				if !wasReady && cancelSome && id%3 == 0 {
					if err := decoder.Cancel(id); err != nil {
						t.Fatal(err)
					}
					expected[id] = qpack.StreamCancellation
					wasCanceled = true
					canceled++
				} else if len(body) > 0 && body[0] != 0 {
					expected[id] = qpack.SectionAcknowledgment
				}
				feed(delayed)
				delayed = nil
				if !wasReady {
					select {
					case decoded = <-result:
					default:
						t.Fatal("section still blocked after all preceding encoder bytes", id)
					}
				}
				if wasCanceled {
					if decoded.Err != h3conn.ErrFieldSectionCanceled || len(decoded.Fields) != 0 {
						t.Fatal(decoded)
					}
				} else if decoded.Err != nil || fieldText(decoded.Fields) != fieldText(lsqpackFields(sections)) {
					t.Fatal(id, decoded)
				}
				sections++
			}
			feed(delayed)
			acknowledgments, cancellations := 0, 0
			for feedback.Len() > 0 {
				instruction, n, err := qpack.ConsumeDecoderInstruction(feedback.Bytes())
				if err != nil {
					t.Fatal(err)
				}
				feedback.Next(n)
				if instruction.Type == qpack.InsertCountIncrement {
					continue
				}
				typ, ok := expected[instruction.Value]
				if !ok || typ != instruction.Type {
					t.Fatal("unexpected or duplicate feedback", instruction)
				}
				delete(expected, instruction.Value)
				if typ == qpack.SectionAcknowledgment {
					acknowledgments++
				} else {
					cancellations++
				}
			}
			wantCanceled := 0
			if cancelSome {
				wantCanceled = 26
			}
			if sections != 80 || blocked != 80 || len(expected) != 0 || canceled != wantCanceled || cancellations != canceled || acknowledgments != 80-wantCanceled {
				t.Fatal(sections, blocked, canceled, len(expected), cancellations)
			}
			t.Logf("sections=%d blocked=%d acknowledgments=%d cancellations=%d", sections, blocked, acknowledgments, cancellations)
		})
	}
}
