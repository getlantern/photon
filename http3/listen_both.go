package http3

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"

	quic "github.com/getlantern/photon"
)

// ListenAndServeTLS serves HTTPS over TCP and HTTP/3 over UDP at the same address
// and advertises HTTP/3 on TCP responses. Empty addr uses :https; port zero picks
// one port shared by both protocols. Nil handler uses http.DefaultServeMux.
// When either server exits, both listeners and their connections are closed.
func ListenAndServeTLS(addr, certFile, keyFile string, handler http.Handler) error {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return err
	}
	if addr == "" {
		addr = ":https"
	}
	tcp, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer tcp.Close()
	return serveTLSAndQUIC(tcp, &tls.Config{Certificates: []tls.Certificate{certificate}}, handler)
}

// tcp is owned by the caller of this helper; every return releases it. The UDP
// listener uses the actual TCP address, including the selected ephemeral port.
func serveTLSAndQUIC(tcp net.Listener, credentials *tls.Config, handler http.Handler) error {
	defer tcp.Close()
	if handler == nil {
		handler = http.DefaultServeMux
	}
	h3 := &Server{Handler: handler}
	configured, err := h3.ConfigureTLSConfig(credentials)
	if err != nil {
		return err
	}
	udp, err := quic.ListenAddrEarly(tcp.Addr().String(), configured, &quic.Config{Allow0RTT: true})
	if err != nil {
		return err
	}
	defer udp.Close()
	defer h3.Close()
	https := &http.Server{TLSConfig: credentials.Clone(), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = h3.SetQUICHeaders(w.Header())
		handler.ServeHTTP(w, r)
	})}
	defer https.Close()
	ready := make(chan struct{})
	quicDone := make(chan error, 1)
	go func() { quicDone <- h3.serveListenerStarted(context.Background(), udp, true, ready) }()
	// No TCP response can race ahead of the initial Alt-Svc publication.
	select {
	case <-ready:
	case err := <-quicDone:
		return err
	}
	tcpDone := make(chan error, 1)
	go func() { tcpDone <- https.ServeTLS(tcp, "", "") }()
	select {
	case err := <-tcpDone:
		h3.Close()
		<-quicDone
		return err
	case err := <-quicDone:
		https.Close()
		<-tcpDone
		return err
	}
}
