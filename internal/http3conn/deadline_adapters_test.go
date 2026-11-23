package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

// scriptedReader returns its steps in order: bytes are data, an error is one
// failed Read, such as an expired read deadline.
type scriptedReader struct{ steps []any }

func (r *scriptedReader) Read(p []byte) (int, error) {
	for len(r.steps) > 0 {
		switch step := r.steps[0].(type) {
		case error:
			r.steps = r.steps[1:]
			return 0, step
		case []byte:
			if len(step) == 0 {
				r.steps = r.steps[1:]
				continue
			}
			n := copy(p, step)
			r.steps[0] = step[n:]
			return n, nil
		}
	}
	return 0, io.EOF
}

// The response body of an ordinary exchange survives a read deadline: the
// request is not reset and the read can be repeated.
func TestExchangeResponseBodyDeadline(t *testing.T) {
	data := readerFrame(t, h3.Data, []byte("abcd"))
	source := &scriptedReader{steps: []any{readerHeaders(t, responseFields("200")...), data[:3], os.ErrDeadlineExceeded, data[3:]}}
	stream, config, done := exchangeFixture(t, source)
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	response, err := ExchangeRequest(context.Background(), request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	deadlines := 0
	for {
		buffer := make([]byte, 8)
		n, err := response.Body.Read(buffer)
		body.Write(buffer[:n])
		if err == io.EOF {
			break
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			if deadlines++; deadlines > 1 {
				t.Fatal("one expired deadline failed every later read")
			}
			continue
		}
		if err != nil {
			t.Fatal("deadline was not retryable:", err)
		}
	}
	if body.String() != "abcd" || deadlines != 1 {
		t.Fatalf("%q after %d deadlines", body.String(), deadlines)
	}
	stream.mu.Lock()
	reads, writes := len(stream.readCodes), len(stream.writeCodes)
	stream.mu.Unlock()
	if reads != 0 || writes != 0 {
		t.Fatal("read deadline reset the request:", reads, writes)
	}
	if err := response.Body.Close(); err != nil || done.Load() != 1 {
		t.Fatal(err, done.Load())
	}
}

// Likewise the request body a handler reads.
func TestServeRequestBodyDeadline(t *testing.T) {
	data := readerFrame(t, h3.Data, []byte("abcd"))
	stream := &serveStream{source: &scriptedReader{steps: []any{readerHeaders(t, requestFields("/")...), data[:3], os.ErrDeadlineExceeded, data[3:]}}}
	var body bytes.Buffer
	deadlines := 0
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for {
			buffer := make([]byte, 8)
			n, err := r.Body.Read(buffer)
			body.Write(buffer[:n])
			if err == io.EOF {
				break
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if r.Context().Err() != nil {
					t.Error("read deadline canceled the request")
					return
				}
				if deadlines++; deadlines > 1 {
					t.Error("one expired deadline failed every later read")
					return
				}
				continue
			}
			if err != nil {
				t.Error("deadline was not retryable:", err)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if body.String() != "abcd" || deadlines != 1 {
		t.Fatalf("%q after %d deadlines", body.String(), deadlines)
	}
	if len(stream.readCodes) != 0 || len(stream.writeCodes) != 0 || stream.closes != 1 {
		t.Fatal("read deadline reset the request:", stream.readCodes, stream.writeCodes, stream.closes)
	}
}

// A deadline can expire while an unknown frame ahead of the response header is
// being skipped. The repeated call finishes skipping it.
func TestReadHTTPResponseRetryInsideSkippedFrame(t *testing.T) {
	unknown := readerFrame(t, 0x21, []byte("skipped"))
	reader := responseReader(t, nil)
	reader.frames = h3.NewReader(&scriptedReader{steps: []any{unknown[:5], os.ErrDeadlineExceeded, unknown[5:], readerHeaders(t, responseFields("200")...)}})
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	config := HTTPResponseConfig{Body: BodyConfig{Close: func(bool) error { return nil }}}
	if _, err := ReadHTTPResponse(context.Background(), request, reader, config); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	response, err := ReadHTTPResponse(context.Background(), request, reader, config)
	if err != nil || response.StatusCode != 200 {
		t.Fatal(response, err)
	}
}

// The bound on interim responses is per response, not per attempt to read it.
func TestClientStreamInformationalLimitSurvivesDeadline(t *testing.T) {
	interim := readerHeaders(t, responseFields("103")...)
	source := &scriptedReader{steps: []any{interim, os.ErrDeadlineExceeded, interim, readerHeaders(t, responseFields("200")...)}}
	session, transport, config, done := rawFixture(t, source)
	config.MaxInformational = 1
	informed := 0
	config.Information = func(int, http.Header) error { informed++; return nil }
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.ReadResponse(); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	_, err = stream.ReadResponse()
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.ExcessiveLoad || informed != 1 {
		t.Fatal("a retry reset the interim response limit:", err, informed)
	}
	awaitDone(t, done)
}
