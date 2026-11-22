package http3

import (
	"errors"
	quic "github.com/getlantern/photon"
	"io"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

// CapsuleType identifies an extension-defined capsule, in the QUIC varint range.
type CapsuleType uint64

const DatagramCapsuleType CapsuleType = 0

var (
	ErrCapsulePayloadPending = h3.ErrPayloadPending
	ErrCapsuleReaderExpired  = errors.New("http3: capsule reader is no longer current")
)

// CapsuleError reports malformed capsule framing. The owning HTTP extension must
// abort the request with Code(), not close the connection.
type CapsuleError struct{ Reason string }

func (e *CapsuleError) Error() string { return "http3: malformed capsule: " + e.Reason }
func (e *CapsuleError) Code() uint64  { return 0x10e }
func (e *CapsuleError) Unwrap() error { return io.ErrUnexpectedEOF }

// CapsuleParser parses an already unframed HTTP data stream, such as a Tunnel.
// Consume or discard each payload before Next. It does not negotiate extensions,
// or enforce message headers. When the source implements CloseWithError (as
// Tunnel and ServerTunnel do), malformed capsules automatically abort that
// request with 0x10e and release tunnel resources. Generic readers report errors
// for their owner to handle. Calls must be serialized. Storage is
// independent of declared payload lengths. Underlying deadlines still apply.
type CapsuleParser struct {
	abort      func(quic.StreamErrorCode, string) error
	reader     *h3.Reader
	generation uint64
	remaining  int64
	err        error
}

func NewCapsuleParser(r io.Reader) *CapsuleParser {
	p := &CapsuleParser{reader: h3.NewCapsuleReader(r)}
	if closer, ok := r.(interface {
		CloseWithError(quic.StreamErrorCode, string) error
	}); ok {
		p.abort = closer.CloseWithError
	}
	return p
}
func (p *CapsuleParser) failure(err error) error {
	if err == nil || errors.Is(err, h3.ErrPayloadPending) {
		return err
	}
	var malformed *h3.Error
	if errors.As(err, &malformed) && malformed.Code == 0x10e {
		err = &CapsuleError{Reason: malformed.Reason}
		p.err = err
		if p.abort != nil {
			_ = p.abort(0x10e, err.Error())
		}
	}
	p.err = err
	return err
}

// Next returns the next type and bounded payload reader. EOF between capsules is
// clean; EOF within a header or payload is a CapsuleError. Failed attempts caused
// by an unconsumed payload leave the current reader usable.
func (p *CapsuleParser) Next() (CapsuleType, CapsuleReader, error) {
	if p.err != nil {
		return 0, CapsuleReader{}, p.err
	}
	header, err := p.reader.Next()
	if err != nil {
		return 0, CapsuleReader{}, p.failure(err)
	}
	p.generation++
	p.remaining = int64(header.Length)
	return CapsuleType(header.Type), CapsuleReader{p, p.generation}, nil
}

// CapsuleReader reads one payload. It expires after Next successfully advances.
// Copies share position; reading is serialized with all parser operations.
type CapsuleReader struct {
	parser     *CapsuleParser
	generation uint64
}

func (r CapsuleReader) valid() bool { return r.parser != nil && r.generation == r.parser.generation }
func (r CapsuleReader) Remaining() int64 {
	if !r.valid() {
		return 0
	}
	return r.parser.remaining
}
func (r CapsuleReader) Read(b []byte) (int, error) {
	if !r.valid() {
		return 0, ErrCapsuleReaderExpired
	}
	p := r.parser
	if p.err != nil {
		return 0, p.err
	}
	n, err := p.reader.Read(b)
	p.remaining -= int64(n)
	if err != nil && err != io.EOF {
		err = p.failure(err)
	}
	return n, err
}
func (r CapsuleReader) ReadByte() (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	return b[0], err
}

// Discard skips a payload without allocating from its declared size.
func (r CapsuleReader) Discard() error {
	if !r.valid() {
		return ErrCapsuleReaderExpired
	}
	p := r.parser
	if p.err != nil {
		if p.err == io.EOF {
			return nil
		}
		return p.err
	}
	// The wire reader already bounds this payload. Avoid boxing a new value
	// reader for each discard; update the shared position after the copy.
	n, err := io.Copy(io.Discard, p.reader)
	p.remaining -= n
	if err != nil {
		return p.failure(err)
	}
	return nil
}

// WriteCapsule writes a complete capsule. The caller serializes writes and must
// abandon or reset the stream after an I/O error, since a partial capsule cannot
// be resumed by calling WriteCapsule again. Invalid types produce no output.
// This accepts any io.Writer, including a Tunnel or ServerTunnel.
func WriteCapsule(w io.Writer, typ CapsuleType, value []byte) error {
	var buffer [16]byte
	header, err := h3.AppendCapsuleHeader(buffer[:0], uint64(typ), uint64(len(value)))
	if err != nil {
		return err
	}
	for _, part := range [][]byte{header, value} {
		if len(part) == 0 {
			continue
		}
		n, err := w.Write(part)
		if err != nil {
			return err
		}
		if n != len(part) {
			return io.ErrShortWrite
		}
	}
	return nil
}
