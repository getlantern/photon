package http3conn

import (
	"context"
	"errors"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

var ErrHTTPDatagramsNotNegotiated = errors.New("http3: datagrams not negotiated")
var ErrDatagramSendClosed = errors.New("http3: datagram send side closed")

// DatagramSink must honor cancellation, enforce QUIC DATAGRAM negotiation and
// size limits, and copy the data before returning if it retains it.
type DatagramSink interface {
	SendDatagramContext(context.Context, []byte) error
}

// SendHTTPDatagram queues an unreliable datagram on a registered extension
// request. Request owners must call CloseSend before FIN/reset. A receive-side FIN
// alone does not prohibit sending on the opposite side of the stream.
func (s *Session) SendHTTPDatagram(ctx context.Context, r *DatagramRequest, payload []byte) error {
	if r == nil || s.datagrams == nil || r.owner != s.datagrams {
		return ErrRequestID
	}
	if s.datagramSender == nil || !s.settings.CanSendHTTPDatagrams() {
		return ErrHTTPDatagramsNotNegotiated
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ctx.Err() != nil {
		return context.Cause(s.ctx)
	}
	sendCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(context.Cause(s.ctx)) })
	stopRequest := context.AfterFunc(r.sendContext, func() { cancel(context.Cause(r.sendContext)) })
	defer func() { stop(); stopRequest(); cancel(context.Canceled) }()
	if r.sendContext.Err() != nil {
		cancel(context.Cause(r.sendContext))
	}
	if s.ctx.Err() != nil {
		cancel(context.Cause(s.ctx))
	}
	select {
	case <-r.sendGate:
	case <-sendCtx.Done():
		return context.Cause(sendCtx)
	}
	defer func() { r.sendGate <- struct{}{} }()
	if sendCtx.Err() != nil {
		return context.Cause(sendCtx)
	}
	if r.sendClosed {
		return ErrDatagramSendClosed
	}
	d := r.owner
	d.mu.Lock()
	active := !d.closed && d.requests[r.id] == r
	supported := r.supported
	d.mu.Unlock()
	if !active {
		return ErrDatagramSendClosed
	}
	if !supported {
		return &DatagramRequestError{ID: r.id}
	}
	data, err := h3.AppendDatagram(nil, r.id, payload)
	if err != nil {
		return err
	}
	err = s.datagramSender.SendDatagramContext(sendCtx, data)
	if err != nil && sendCtx.Err() != nil {
		return context.Cause(sendCtx)
	}
	return err
}
