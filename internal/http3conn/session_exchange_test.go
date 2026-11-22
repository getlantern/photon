package http3conn

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func managedExchangeSession(t *testing.T, config ClientExchangeConfig) *Session {
	t.Helper()
	s, _ := requestLoopSession(t, ServeRequestConfig{Encoder: config.Encoder, Decoder: config.Decoder})
	s.server = false
	s.maxRequests = 1
	return s
}
func TestManagedExchangeRetainsAdmissionUntilBodyClose(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(readerHeaders(t, responseFields("200")...)))
	session := managedExchangeSession(t, config)
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	response, err := session.ExchangeRequest(request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if _, err := session.BeginRequest(4); !errors.Is(err, ErrRequestLimit) {
		t.Fatal("body EOF freed admission", err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if done.Load() != 1 {
		t.Fatal(done.Load())
	}
	lease, err := session.BeginRequest(4)
	if err != nil {
		t.Fatal("body close did not release admission", err)
	}
	lease.Finish()
}
func TestManagedExchangeGoawayRejectsBlockedUploadAndResponse(t *testing.T) {
	responsePipe, responseWriter := io.Pipe()
	defer responsePipe.Close()
	defer responseWriter.Close()
	source := io.MultiReader(bytes.NewReader(readerHeaders(t, responseFields("200")...)), responsePipe)
	stream, config, done := exchangeFixture(t, source)
	stream.interrupt = func() { responsePipe.CloseWithError(io.ErrClosedPipe) }
	session := managedExchangeSession(t, config)
	uploadPipe, producer := io.Pipe()
	defer uploadPipe.Close()
	defer producer.Close()
	entered := make(chan struct{})
	body := &uploadBody{read: func(p []byte) (int, error) { close(entered); return uploadPipe.Read(p) }, close: uploadPipe.Close}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	response, err := session.ExchangeRequest(request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upload missing")
	}
	session.receiveGoaway(PeerGoaway{ID: 0, RequestIDs: true})
	if _, err := io.ReadAll(response.Body); !errors.Is(err, ErrRequestRejected) {
		t.Fatal("GOAWAY rejection lost", err)
	}
	session.requestsMu.Lock()
	retained := len(session.requests)
	session.requestsMu.Unlock()
	if retained != 1 || done.Load() != 0 {
		t.Fatal("rejected exchange released before cleanup", retained, done.Load())
	}
	if err := response.Body.Close(); !errors.Is(err, ErrRequestRejected) {
		t.Fatal(err)
	}
	session.requestsMu.Lock()
	retained = len(session.requests)
	session.requestsMu.Unlock()
	if retained != 0 || done.Load() != 1 || body.closes.Load() != 1 {
		t.Fatal(retained, done.Load(), body.closes.Load())
	}
}
func TestManagedExchangeDrainingRefusesNewRequest(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(nil))
	rejected := 0
	config.RejectedBeforeSend = func() { rejected++ }
	session := managedExchangeSession(t, config)
	session.receiveGoaway(PeerGoaway{ID: 8, RequestIDs: true})
	body := &uploadBody{read: func([]byte) (int, error) { t.Error("rejected upload read"); return 0, io.EOF }}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	if response, err := session.ExchangeRequest(request, stream, config); response != nil || !errors.Is(err, ErrPeerDraining) {
		t.Fatal(response, err)
	}
	if rejected != 1 || stream.Len() != 0 || done.Load() != 1 || body.closes.Load() != 1 || len(stream.writeCodes) != 1 || stream.writeCodes[0] != 0x10b {
		t.Fatal("admission failure ownership", done.Load(), body.closes.Load(), stream.writeCodes)
	}
}

func TestManagedExchangeGoawayPreservesLowerRequest(t *testing.T) {
	wire := readerHeaders(t, responseFields("200")...)
	wire = append(wire, readerFrame(t, 0, []byte("accepted"))...)
	stream, config, done := exchangeFixture(t, bytes.NewReader(wire))
	session := managedExchangeSession(t, config)
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	response, err := session.ExchangeRequest(request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	session.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
	if _, err := session.BeginRequest(4); !errors.Is(err, ErrPeerDraining) {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "accepted" {
		t.Fatal("lower request canceled by GOAWAY", string(body), err)
	}
	if err := response.Body.Close(); err != nil || done.Load() != 1 {
		t.Fatal(err, done.Load())
	}
}
