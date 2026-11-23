package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestExtendedConnectRequestFields(t *testing.T) {
	request, _ := http.NewRequest("CONNECT", "https://example.com/chat%2Froom?q=1", nil)
	extended, err := WithExtendedConnectProtocol(request, "echo-test")
	if err != nil {
		t.Fatal(err)
	}
	fields, err := HTTPRequestHeaders(extended)
	if err != nil {
		t.Fatal(err)
	}
	control, err := ValidateControlData(fields, RequestFields, true)
	if err != nil || control.Protocol != "echo-test" || control.Path != "/chat%2Froom?q=1" || control.Scheme != "https" || control.Authority != "example.com" {
		t.Fatal(control, err)
	}
	if ExtendedConnectProtocol(request) != "" || ExtendedConnectProtocol(extended.Clone(extended.Context())) != "echo-test" {
		t.Fatal("request metadata mutation")
	}
	for _, protocol := range []string{"", "bad protocol", "x\r\ny", "bad:token"} {
		if _, err := WithExtendedConnectProtocol(request, protocol); err == nil {
			t.Fatal(protocol)
		}
	}
	request.Method = "GET"
	if _, err := WithExtendedConnectProtocol(request, "echo-test"); err == nil {
		t.Fatal("protocol allowed without CONNECT")
	}
}

func TestManagedExtendedConnectNegotiation(t *testing.T) {
	for _, mode := range []string{"missing", "disabled", "enabled", "waiting"} {
		t.Run(mode, func(t *testing.T) {
			stream, config, done := exchangeFixture(t, bytes.NewReader(readerHeaders(t, responseFields("200")...)))
			config.AllowConnect = true
			config.ExtendedConnect = true // session must replace caller's assertion
			session := managedExchangeSession(t, config)
			session.settings, _ = NewSettingsState(false, nil)
			var settings []h3.Setting
			if mode == "disabled" {
				settings = []h3.Setting{{ID: h3.EnableConnectProtocol, Value: 0}}
			}
			if mode == "enabled" {
				settings = []h3.Setting{{ID: h3.EnableConnectProtocol, Value: 1}}
			}
			if mode == "waiting" {
				session.ready = make(chan struct{})
				session.encoder.Store(nil)
			} else if err := session.settings.ApplyPeer(settings); err != nil {
				t.Fatal(err)
			}
			timeout := time.Second
			if mode == "waiting" {
				timeout = 20 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, "CONNECT", "https://example.com/chat", nil)
			request, _ = WithExtendedConnectProtocol(request, "echo-test")
			response, err := session.ExchangeRequest(request, stream, config)
			if mode == "enabled" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadAll(response.Body); err != nil {
					t.Fatal(err)
				}
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				want := ErrExtendedConnectNotNegotiated
				if mode == "waiting" {
					want = context.DeadlineExceeded
				}
				if response != nil || !errors.Is(err, want) {
					t.Fatal(response, err)
				}
				if stream.Len() != 0 {
					t.Fatal("request bytes sent before permission")
				}
			}
			if done.Load() != 1 {
				t.Fatal("request cleanup", done.Load())
			}
		})
	}
}

func TestServeExtendedConnectRouting(t *testing.T) {
	for _, mode := range []string{"known", "unknown", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := http.NewRequest("CONNECT", "https://example.com/chat?q=1", nil)
			r, _ = WithExtendedConnectProtocol(r, "echo-test")
			fields, err := HTTPRequestHeaders(r)
			if err != nil {
				t.Fatal(err)
			}
			stream := &serveStream{source: bytes.NewReader(readerHeaders(t, fields...))}
			called := false
			config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("extended request reached ordinary handler") }))
			config.ExtendedConnect = mode != "disabled"
			if mode == "known" {
				config.ExtendedConnectHandlers = map[string]http.Handler{"echo-test": http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					if ExtendedConnectProtocol(r) != "echo-test" || r.Proto != "HTTP/3.0" || r.RequestURI != "/chat?q=1" || r.URL.Scheme != "" || r.URL.Host != "" || r.Host != "example.com" {
						t.Error("request metadata", r)
					}
					if _, err := w.(*HTTPResponseWriter).AcceptTunnel(); err != nil {
						t.Error(err)
					}
				})}
			}
			err = ServeRequest(context.Background(), stream, config)
			if mode == "disabled" {
				var message *MessageError
				if !errors.As(err, &message) || stream.Len() != 0 {
					t.Fatal("unnegotiated request", err)
				}
				return
			}
			if err != nil || called != (mode == "known") {
				t.Fatal(err, called)
			}
			reader := responseReader(t, stream.Bytes())
			event, err := reader.Next(context.Background())
			want := 200
			if mode == "unknown" {
				want = 501
			}
			if err != nil || event.Control.Status != want {
				t.Fatal(event, err)
			}
		})
	}
}
