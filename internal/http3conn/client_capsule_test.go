package http3conn

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestClientCapsuleResponsePolicy(t *testing.T) {
	for _, scenario := range []string{"valid", "length", "type", "status204", "status205", "status206", "rejection", "rejection-header", "informational-header"} {
		t.Run(scenario, func(t *testing.T) {
			status := "200"
			switch scenario {
			case "status204":
				status = "204"
			case "status205":
				status = "205"
			case "status206":
				status = "206"
			case "rejection", "rejection-header":
				status = "403"
			}
			fields := responseFields(status)
			switch scenario {
			case "length":
				fields = append(fields, fs("content-length", "0"))
			case "type", "rejection":
				fields = append(fields, fs("content-type", "text/plain"))
			case "rejection-header":
				fields = append(fields, fs("capsule-protocol", "?1"))
			}
			source := readerHeaders(t, fields...)
			if scenario == "informational-header" {
				source = append(readerHeaders(t, append(responseFields("103"), fs("capsule-protocol", "?1"))...), source...)
			}
			stream, config, done := exchangeFixture(t, bytes.NewReader(source))
			config.Capsules = true
			config.ExtendedConnect = true
			config.AllowConnect = true
			request, _ := http.NewRequest("CONNECT", "https://example.org/capsules", nil)
			request, _ = WithExtendedConnectProtocol(request, "capsule-test")
			response, err := ExchangeRequest(context.Background(), request, stream, config)
			if scenario == "valid" || scenario == "rejection" {
				if err != nil {
					t.Fatal(err)
				}
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				if response != nil {
					response.Body.Close()
					t.Fatal("accepted malformed response")
				}
				var message *MessageError
				if !errors.As(err, &message) {
					t.Fatalf("wrong error: %T %v", err, err)
				}
				if len(stream.readCodes) == 0 || stream.readCodes[0] != 0x10e || len(stream.writeCodes) == 0 || stream.writeCodes[0] != 0x10e {
					t.Fatal(stream.readCodes, stream.writeCodes)
				}
			}
			if done.Load() != 1 {
				t.Fatal("exchange ownership leaked", done.Load())
			}
		})
	}
}

func TestClientCapsuleRequestPreflight(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(readerHeaders(t, responseFields("200")...)))
	config.Capsules = true
	config.ExtendedConnect = true
	config.AllowConnect = true
	request, _ := http.NewRequest("CONNECT", "https://example.org/capsules", nil)
	request, _ = WithExtendedConnectProtocol(request, "capsule-test")
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := ExchangeRequest(context.Background(), request, stream, config)
	var message *MessageError
	if response != nil || !errors.As(err, &message) {
		t.Fatal(response, err)
	}
	if stream.Len() != 0 || done.Load() != 1 {
		t.Fatal("invalid request sent or ownership leaked", stream.Len(), done.Load())
	}
}
