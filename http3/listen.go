package http3

import (
	"context"
	"crypto/tls"
	"net/http"

	quic "github.com/getlantern/photon"
)

// ListenAndServe owns a UDP listener at Addr and serves HTTP/3 until shutdown.
// TLSConfig must be set. Shutdown and Close release the listener's socket.
func (s *Server) ListenAndServe() error { return s.listenWithTLS(s.TLSConfig) }

// ListenAndServeTLS loads the certificate and key files and serves HTTP/3 at
// Addr. Like the reference convenience API, file credentials supply a fresh TLS
// configuration; this method does not use TLSConfig's other fields or callbacks.
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	return s.listenWithTLS(&tls.Config{Certificates: []tls.Certificate{certificate}})
}

func (s *Server) listenWithTLS(source *tls.Config) error {
	configured, err := s.ConfigureTLSConfig(source)
	if err != nil {
		return err
	}
	s.mu.Lock()
	stopped := s.stopping
	s.mu.Unlock()
	if stopped {
		return ErrServerClosed
	}
	transport := &quic.Config{Allow0RTT: true}
	if s.QUICConfig != nil {
		transport = s.QUICConfig.Clone()
	}
	if s.EnableDatagrams || len(s.DatagramHandlers) != 0 {
		transport.EnableDatagrams = true
	}
	address := s.Addr
	if address == "" {
		address = ":https"
	}
	listener, err := quic.ListenAddrEarly(address, configured, transport)
	if err != nil {
		return err
	}
	// Also cover rejection before serveListener registers ownership (for example
	// a concurrent Close). Close is idempotent after the owned serve loop finishes.
	defer listener.Close()
	return s.serveListener(context.Background(), listener, true)
}

// ListenAndServeQUIC serves HTTP/3 with file credentials and an optional handler.
// A nil handler uses http.DefaultServeMux.
func ListenAndServeQUIC(addr, certFile, keyFile string, handler http.Handler) error {
	server := &Server{Addr: addr, Handler: handler}
	return server.ListenAndServeTLS(certFile, keyFile)
}
