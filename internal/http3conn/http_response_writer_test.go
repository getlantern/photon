package http3conn

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
)

func handlerWriter(t *testing.T, sink *messageSink, method string) *HTTPResponseWriter {
	t.Helper()
	wire := responseWriter(t, sink, func(name string) bool { return name == "x-checksum" || name == "x-late" })
	wire.message.config.RequestMethod = method
	w, err := NewHTTPResponseWriter(wire)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestHTTPResponseWriterHandlerRoundTrip(t *testing.T) {
	sink := new(messageSink)
	w := handlerWriter(t, sink, "GET")
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.WriteHeader(103)
		w.Header().Del("Link")
		w.Header().Set("Trailer", "X-Checksum")
		w.Header().Set("X-Checksum", "not an initial header")
		w.Header().Set("Content-Length", "3")
		w.Header().Set("X-Snapshot", "before")
		w.WriteHeader(201)
		w.Header().Set("X-Snapshot", "after")
		w.WriteHeader(500)
		if n, err := w.Write([]byte("abc")); n != 3 || err != nil {
			t.Fatal(n, err)
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("X-Checksum", "ok")
		w.Header().Set(http.TrailerPrefix+"X-Late", "late")
	}).ServeHTTP(w, &http.Request{Method: "GET"})
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil || sink.closes != 1 {
		t.Fatal(err, sink.closes)
	}
	if _, err := w.Write(nil); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
	interim := 0
	response, err := ReadHTTPResponse(context.Background(), &http.Request{Method: "GET"}, responseReader(t, sink.Bytes()), HTTPResponseConfig{MaxInformational: 1, Information: func(code int, h http.Header) error {
		interim++
		if code != 103 || h.Get("Link") == "" {
			t.Fatal(code, h)
		}
		return nil
	}, Body: BodyConfig{Close: func(bool) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "abc" {
		t.Fatal(string(body), err)
	}
	if interim != 1 || response.StatusCode != 201 || response.Header.Get("X-Snapshot") != "before" || response.Header.Get("X-Checksum") != "" || response.Header.Get("Content-Type") != "text/plain; charset=utf-8" || response.Trailer.Get("X-Checksum") != "ok" || response.Trailer.Get("X-Late") != "late" {
		t.Fatal(response)
	}
}
func TestHTTPResponseWriterBodyless(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
	}{{"HEAD", 200}, {"GET", 204}, {"GET", 205}, {"GET", 304}} {
		t.Run(tc.method+http.StatusText(tc.status), func(t *testing.T) {
			sink := new(messageSink)
			w := handlerWriter(t, sink, tc.method)
			w.Header()["Date"] = nil
			if tc.method == "HEAD" {
				w.Header().Set("Content-Length", "123")
			}
			w.WriteHeader(tc.status)
			n, err := w.Write([]byte("abc"))
			if tc.method == "HEAD" {
				if n != 3 || err != nil {
					t.Fatal(n, err)
				}
			} else if n != 0 || !errors.Is(err, http.ErrBodyNotAllowed) {
				t.Fatal(n, err)
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			reader := responseReader(t, sink.Bytes())
			reader.message.config.RequestMethod = tc.method
			event, err := reader.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range event.Fields {
				if field.Name == "date" {
					t.Fatal("Date suppression ignored")
				}
			}
			if _, err := reader.Next(context.Background()); err != io.EOF {
				t.Fatal(err)
			}
		})
	}
}
func TestHTTPResponseWriterFailures(t *testing.T) {
	for _, mode := range []string{"short-write", "length", "trailer", "101"} {
		t.Run(mode, func(t *testing.T) {
			sink := new(messageSink)
			w := handlerWriter(t, sink, "GET")
			switch mode {
			case "short-write":
				sink.failCall = 1
				w.Flush()
			case "length":
				w.Header().Set("Content-Length", "1")
				w.Write([]byte("too long"))
			case "trailer":
				w.Header().Set(http.TrailerPrefix+"Content-Length", "0")
				w.Finish()
			case "101":
				w.WriteHeader(101)
			}
			err := w.Err()
			if err == nil {
				t.Fatal("missing failure")
			}
			calls := sink.calls
			w.WriteHeader(200)
			w.Write([]byte("retry"))
			w.Flush()
			if w.Finish() != err || sink.calls != calls || sink.closes != 0 {
				t.Fatal("failure retried or FIN sent")
			}
		})
	}
}
func TestHTTPResponseWriterEmptyAndImplicit(t *testing.T) {
	for _, write := range []bool{false, true} {
		sink := new(messageSink)
		w := handlerWriter(t, sink, "GET")
		if write {
			if _, err := w.Write([]byte("ok")); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		event, err := responseReader(t, sink.Bytes()).Next(context.Background())
		if err != nil || event.Control.Status != 200 {
			t.Fatal(event, err)
		}
	}
}

func TestHTTPResponseWriterFlushAndSuppression(t *testing.T) {
	sink := new(messageSink)
	w := handlerWriter(t, sink, "GET")
	w.Header()["Content-Type"] = nil
	w.Header().Set("X-Before", "yes")
	if err := http.NewResponseController(w).Flush(); err != nil {
		t.Fatal(err)
	}
	w.Header().Set("X-After", "no")
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	event, err := responseReader(t, sink.Bytes()).Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range event.Fields {
		if field.Name == "content-type" || field.Name == "x-after" {
			t.Fatal("header changed after flush", field)
		}
	}
	if err := w.FlushError(); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
}
func TestHTTPResponseWriterFinishRejectsShortBody(t *testing.T) {
	sink := new(messageSink)
	w := handlerWriter(t, sink, "GET")
	w.Header().Set("Content-Length", "4")
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err == nil || sink.closes != 0 {
		t.Fatal("incomplete body sent FIN", err)
	}
	calls := sink.calls
	if err := w.Finish(); err == nil || sink.calls != calls {
		t.Fatal("retried incomplete body", err)
	}
}
func TestHTTPResponseWriterInvalidStatus(t *testing.T) {
	w := handlerWriter(t, new(messageSink), "GET")
	defer func() {
		if recover() == nil {
			t.Fatal("invalid status did not panic")
		}
	}()
	w.WriteHeader(99)
}

func TestHTTPResponseWriterErrorMappingPreservesCause(t *testing.T) {
	for _, flush := range []bool{false, true} {
		sink := &messageSink{failCall: 1}
		w := handlerWriter(t, sink, "GET")
		mapped := errors.New("handler-facing error")
		var original error
		w.mapError = func(err error) error { original = err; return mapped }
		var err error
		if flush {
			err = w.FlushError()
		} else {
			_, err = w.Write([]byte("x"))
		}
		if err != mapped || original == nil || w.err != original || w.Err() != mapped {
			t.Fatal(err, original, w.err)
		}
		if err := w.Finish(); err != original {
			t.Fatal("lifecycle lost original failure", err)
		}
	}
}
