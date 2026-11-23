package http3conn

import (
	"context"
	"errors"
	"io"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3wire"
)

// ReceiveStream deadlines must interrupt an in-progress Read.
type ReceiveStream interface {
	io.Reader
	StreamID() quic.StreamID
	SetReadDeadline(time.Time) error
	CancelRead(quic.StreamErrorCode)
}
type Incoming struct{ registry *http3wire.UniRegistry }

func NewIncoming(peerIsServer bool) *Incoming {
	return &Incoming{registry: http3wire.NewUniRegistry(peerIsServer)}
}

// Classify consumes only the type prefix. known=false,nil means the stream was
// closed/reset before its type completed, or an unknown type was stopped with
// STOP_SENDING. Known streams transfer to the caller for control/QPACK/push
// dispatch. Protocol errors require connection closure by the owner. Calls for
// distinct streams may run concurrently; the owner must bound their number.
// Classification owns and clears the read deadline; the handler sets its own.
// A canceled/failed prefix read is not retryable: some prefix bytes may be gone.
func (in *Incoming) Classify(ctx context.Context, stream ReceiveStream) (typ uint64, known bool, err error) {
	if err = ctx.Err(); err != nil {
		return 0, false, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); stream.SetReadDeadline(time.Now()) })
	typ, err = http3wire.ReadUniType(stream)
	if !stop() {
		<-done
	}
	clearErr := stream.SetReadDeadline(time.Time{})
	if ctx.Err() != nil {
		return 0, false, ctx.Err()
	}
	if err != nil {
		if peerStreamClosed(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if clearErr != nil {
		return 0, false, clearErr
	}
	known, err = in.registry.Register(uint64(stream.StreamID()), typ)
	if err != nil {
		return 0, false, err
	}
	if !known {
		stream.CancelRead(quic.StreamErrorCode(http3wire.StreamCreationError))
	}
	return typ, known, nil
}

// ReadError maps confirmed peer FIN/reset on registered critical streams to
// H3_CLOSED_CRITICAL_STREAM. Deadlines, local cancellations and connection errors
// retain their cause. Push streams retain their original closure indication.
func (in *Incoming) ReadError(id quic.StreamID, err error) error {
	if err != nil && peerStreamClosed(err) {
		if closed := in.registry.Closed(uint64(id)); closed != nil {
			return closed
		}
	}
	return err
}
func peerStreamClosed(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var reset *quic.StreamError
	return errors.As(err, &reset) && reset.Remote
}
