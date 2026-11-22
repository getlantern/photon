package http3conn

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestServerCapsulePolicy(t *testing.T) {
	for _, scenario := range []string{"valid", "request-type", "request-length", "response-type", "response-length", "status204", "status205", "status206", "rejection", "rejection-header", "unselected"} {
		t.Run(scenario, func(t *testing.T) {
			fields := requestControl("CONNECT", "https", "example.org", "/capsules")
			fields = append(fields, fs(":protocol", "capsule-test"))
			if scenario == "request-type" {
				fields = append(fields, fs("content-type", "text/plain"))
			}
			if scenario == "request-length" {
				fields = append(fields, fs("content-length", "0"))
			}
			stream := &serveStream{source: bytes.NewReader(readerHeaders(t, fields...))}
			called := false
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				switch scenario {
				case "response-type", "unselected":
					w.Header().Set("Content-Type", "text/plain")
				case "response-length":
					w.Header().Set("Content-Length", "0")
				case "status204":
					w.WriteHeader(204)
				case "status205":
					w.WriteHeader(205)
				case "status206":
					w.WriteHeader(206)
				case "rejection":
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(403)
					return
				case "rejection-header":
					w.Header().Set("Capsule-Protocol", "?1")
					w.WriteHeader(403)
					return
				}
				// Response validation occurs in the normal writer, including handlers which
				// choose Flush instead of AcceptTunnel. A failed commit must reset the request.
				w.(http.Flusher).Flush()
			}))
			config.ExtendedConnect = true
			config.ExtendedConnectHandlers = map[string]http.Handler{"capsule-test": config.Handler}
			config.CapsuleProtocols = map[string]bool{"capsule-test": scenario != "unselected"}
			err := ServeRequest(context.Background(), stream, config)
			valid := scenario == "valid" || scenario == "rejection" || scenario == "unselected"
			if valid {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var message *MessageError
			if !errors.As(err, &message) {
				t.Fatalf("wrong error scope: %T %v", err, err)
			}
			if (scenario == "request-type" || scenario == "request-length") && called {
				t.Fatal("invalid headers reached handler")
			}
			if len(stream.readCodes) == 0 || stream.readCodes[0] != 0x10e || len(stream.writeCodes) == 0 || stream.writeCodes[0] != 0x10e {
				t.Fatal(stream.readCodes, stream.writeCodes)
			}
		})
	}
}
