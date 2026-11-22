package http3

import (
	"net/http"
	"testing"
)

func TestExtendedTunnelRejectsInvalidProtocol(t *testing.T) {
	for _, protocol := range []string{"", "bad protocol", "bad:token", "x\r\ny"} {
		request, _ := http.NewRequest("CONNECT", "https://example.com/chat", nil)
		if tunnel, response, err := new(ClientConn).OpenExtendedTunnel(request, protocol); err == nil || tunnel != nil || response != nil {
			t.Fatal(protocol, tunnel, response, err)
		}
	}
}
func TestExtendedConnectHandlerConfiguration(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, handlers := range []map[string]http.Handler{{"": handler}, {"bad protocol": handler}, {"echo-test": nil}} {
		server := &Server{ExtendedConnectHandlers: handlers}
		if _, _, _, err := server.limits(); err == nil {
			t.Fatal("invalid registry", handlers)
		}
	}
	if _, _, _, err := (&Server{ExtendedConnectHandlers: map[string]http.Handler{"echo-test": handler}}).limits(); err != nil {
		t.Fatal(err)
	}
}

func TestDatagramClientConfiguration(t *testing.T) {
	for _, cfg := range []*ClientConfig{{MaxDatagramQueue: -1}, {MaxDatagramPayload: -1}, {MaxDatagramPayload: 65536}} {
		if _, err := clientOptions(cfg); err == nil {
			t.Fatal("invalid datagram limits", cfg)
		}
	}
	cfg, err := clientOptions(&ClientConfig{EnableDatagrams: true})
	if err != nil || cfg.MaxDatagramQueue != 16 || cfg.MaxDatagramPayload != 1200 {
		t.Fatal(cfg, err)
	}
	request, _ := http.NewRequest("CONNECT", "https://example.com/echo", nil)
	if tunnel, response, err := new(ClientConn).OpenDatagramTunnel(request, "echo-test"); err != ErrHTTPDatagramsNotNegotiated || tunnel != nil || response != nil {
		t.Fatal(tunnel, response, err)
	}
}

func TestDatagramServerConfiguration(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, server := range []*Server{
		{DatagramHandlers: map[string]http.Handler{"": handler}},
		{DatagramHandlers: map[string]http.Handler{"echo-test": nil}},
		{DatagramHandlers: map[string]http.Handler{"echo-test": handler}, ExtendedConnectHandlers: map[string]http.Handler{"echo-test": handler}},
		{MaxDatagramQueue: -1}, {MaxDatagramPayload: 65536},
	} {
		if _, _, _, err := server.limits(); err == nil {
			t.Fatal("invalid datagram server configuration")
		}
	}
	if _, _, _, err := (&Server{DatagramHandlers: map[string]http.Handler{"echo-test": handler}}).limits(); err != nil {
		t.Fatal(err)
	}
}

func TestCapsuleServerConfiguration(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, server := range []*Server{
		{CapsuleProtocols: map[string]bool{"capsule-test": true}},
		{CapsuleProtocols: map[string]bool{"bad token": true}},
	} {
		if _, _, _, err := server.limits(); err == nil {
			t.Fatal("unregistered capsule protocol accepted")
		}
	}
	for _, datagram := range []bool{false, true} {
		server := &Server{CapsuleProtocols: map[string]bool{"capsule-test": true}}
		if datagram {
			server.DatagramHandlers = map[string]http.Handler{"capsule-test": handler}
		} else {
			server.ExtendedConnectHandlers = map[string]http.Handler{"capsule-test": handler}
		}
		if _, _, _, err := server.limits(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCapsuleClientConfiguration(t *testing.T) {
	for _, token := range []string{"", "bad token"} {
		if _, err := clientOptions(&ClientConfig{CapsuleProtocols: map[string]bool{token: true}}); err == nil {
			t.Fatal("invalid protocol accepted")
		}
	}
	original := &ClientConfig{CapsuleProtocols: map[string]bool{"capsule-test": true}}
	copied, err := clientOptions(original)
	if err != nil {
		t.Fatal(err)
	}
	original.CapsuleProtocols["capsule-test"] = false
	if !copied.CapsuleProtocols["capsule-test"] {
		t.Fatal("policy map aliases caller")
	}
}
