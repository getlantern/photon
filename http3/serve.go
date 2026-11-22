package http3

import (
	"context"
	"fmt"
	"net"
	"sync"

	quic "github.com/getlantern/photon"
)

// QUICListener accepts QUIC connections for an HTTP/3 server. Both Listener and
// EarlyListener implement this interface. Accept must respect cancellation.
type QUICListener interface {
	Accept(context.Context) (*quic.Conn, error)
	Addr() net.Addr
	Close() error
}

var _ QUICListener = (*quic.Listener)(nil)
var _ QUICListener = (*quic.EarlyListener)(nil)

type serverListener struct {
	listener  QUICListener
	owned     bool
	port      int
	order     uint64
	advertise bool
	stop      context.CancelFunc
	done      chan struct{}
}

// Serve owns listener after validation/registration, accepts established QUIC
// connections and serves each with ServeQUICConn. Configure TLS ALPN as h3 when
// creating the listener. Concurrent connection tasks are bounded per listener;
// MaxConnections also limits admitted connections across the entire Server.
// Shutdown stops acceptance without closing the shared UDP socket until active
// connections finish. Close or ctx cancellation aborts active work. Serve joins
// its workers before returning and returns ErrServerClosed after server shutdown.
func (s *Server) Serve(parent context.Context, listener *quic.Listener) error {
	if listener == nil {
		return fmt.Errorf("http3: nil listener")
	}
	return s.serveListener(parent, listener, true)
}

// ServeListener serves an application-owned QUIC listener. Shutdown and Close
// stop this accept loop and its HTTP/3 connections without closing the listener.
// The caller must close the listener. Configure h3 ALPN before creating it.
// After Shutdown or Close it returns http.ErrServerClosed. Returning from this
// method does not wait for existing connections; use Shutdown to join them.
func (s *Server) ServeListener(listener QUICListener) error {
	return s.serveListener(context.Background(), listener, false)
}

func (s *Server) serveListener(parent context.Context, listener QUICListener, owned bool) (result error) {
	return s.serveListenerStarted(parent, listener, owned, nil)
}

// ready signals that registration and Alt-Svc publication have completed.
func (s *Server) serveListenerStarted(parent context.Context, listener QUICListener, owned bool, ready chan<- struct{}) (result error) {
	_, _, limit, err := s.limits()
	if err != nil {
		return err
	}
	if parent == nil || listener == nil {
		return fmt.Errorf("http3: nil serving context or listener")
	}
	// A selector can enable reception per connection. ServeQUICConn checks
	// each accepted connection's selected policy before initializing HTTP/3.
	if configured, ok := listener.(interface{ DatagramsEnabled() bool }); ok && (s.EnableDatagrams || len(s.DatagramHandlers) != 0) && !configured.DatagramsEnabled() {
		return fmt.Errorf("http3: QUIC listener datagram reception is disabled")
	}
	if parent.Err() != nil {
		return context.Cause(parent)
	}
	ctx, cancel := context.WithCancelCause(parent)
	acceptCtx, stopAccept := context.WithCancel(ctx)
	entry := &serverListener{listener: listener, owned: owned, stop: stopAccept, done: make(chan struct{})}
	if address := listener.Addr(); address != nil {
		entry.port = altSvcPort(address.String())
	}
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		cancel(nil)
		stopAccept()
		return ErrServerClosed
	}
	// Preserve duplicate detection for the concrete listener accepted by Serve.
	// Entries themselves are keys so custom non-comparable listener values work.
	if concrete, ok := listener.(*quic.Listener); ok {
		for existing := range s.listeners {
			if prior, ok := existing.listener.(*quic.Listener); ok && prior == concrete {
				s.mu.Unlock()
				cancel(nil)
				stopAccept()
				return fmt.Errorf("http3: listener already served")
			}
		}
	}
	if s.listeners == nil {
		s.listeners = make(map[*serverListener]struct{})
	}
	s.listeners[entry] = struct{}{}
	s.listenerOrder++
	entry.order, entry.advertise = s.listenerOrder, true
	s.refreshAltSvcLocked()
	s.mu.Unlock()
	if ready != nil {
		close(ready)
	}
	var workers sync.WaitGroup
	defer func() {
		stopAccept()
		s.mu.Lock()
		stopping := s.stopping
		entry.advertise = false
		s.refreshAltSvcLocked()
		s.mu.Unlock()
		if stopping {
			result = ErrServerClosed
		} else if owned {
			cancel(result)
		}
		finish := func() {
			workers.Wait()
			cancel(result)
			if owned {
				listener.Close()
			}
			s.mu.Lock()
			delete(s.listeners, entry)
			close(entry.done)
			s.mu.Unlock()
		}
		if owned {
			finish()
		} else {
			// Keep worker ownership registered for Shutdown, but let the
			// application's accept loop return independently of handlers.
			go finish()
		}
	}()
	slots := make(chan struct{}, limit)
	for {
		select {
		case slots <- struct{}{}:
		case <-acceptCtx.Done():
			return context.Cause(acceptCtx)
		}
		if acceptCtx.Err() != nil {
			return context.Cause(acceptCtx)
		}
		conn, err := listener.Accept(acceptCtx)
		if err != nil {
			return err
		}
		if acceptCtx.Err() != nil {
			conn.CloseWithError(0x100, "HTTP3 listener stopped")
			return context.Cause(acceptCtx)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			// Admission/ALPN failures leave conn caller-owned. This accept loop is
			// that caller, so it closes failures as well as completed connections.
			defer conn.CloseWithError(0x100, "HTTP3 serving completed")
			if err := s.ServeQUICConn(ctx, conn); err != nil && s.Logger != nil {
				s.Logger.Debug("http3: handling connection failed", "remote", conn.RemoteAddr(), "error", err)
			}
		}()
	}
}
