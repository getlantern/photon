package http3

import (
	"context"
	"net/http"

	quic "github.com/getlantern/photon"
)

type contextKey struct{ name string }

func (k *contextKey) String() string { return "quic-go/http3 context value " + k.name }

// ServerContextKey identifies the *Server serving a handler's connection.
var ServerContextKey = &contextKey{name: "http3-server"}

// RemoteAddrContextKey identifies the connection's remote net.Addr at setup.
var RemoteAddrContextKey = &contextKey{name: "remote-addr"}

func (s *Server) connectionContext(base context.Context, conn *quic.Conn) (context.Context, func()) {
	ctx := context.WithValue(base, ServerContextKey, s)
	ctx = context.WithValue(ctx, http.LocalAddrContextKey, conn.LocalAddr())
	ctx = context.WithValue(ctx, RemoteAddrContextKey, conn.RemoteAddr())
	if s.ConnContext == nil {
		return ctx, func() {}
	}
	selected := s.ConnContext(ctx, conn)
	if selected == nil {
		panic("http3: ConnContext returned nil")
	}
	return bindServerContext(base, selected)
}

// A callback may replace rather than derive its context. Keep server/transport
// shutdown authoritative even then, while retaining callback values, deadlines
// and cancellation. No locks are held while invoking application code.
func bindServerContext(base, selected context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(selected)
	done := make(chan struct{})
	stop := context.AfterFunc(base, func() { defer close(done); cancel(context.Cause(base)) })
	if base.Err() != nil {
		cancel(context.Cause(base))
	}
	return ctx, func() {
		if !stop() {
			<-done
		}
		cancel(context.Canceled)
	}
}
