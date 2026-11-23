package http3conn

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestHTTPResponseAdapter(t *testing.T) {
	data := readerHeaders(t, fs(":status", "103"), fs("link", "</style.css>; rel=preload"))
	data = append(data, readerHeaders(t, fs(":status", "200"), fs("content-length", "03, 3"), fs("content-length", "3"), fs("set-cookie", "a=1"), fs("set-cookie", "b=2"), fs("trailer", "X-Sum"))...)
	data = append(data, readerFrame(t, h3.Data, []byte("abc"))...)
	data = append(data, readerHeaders(t, fs("x-sum", "ok"), fs("x-extra", "separate"))...)
	request := &http.Request{Method: "GET"}
	closed, interim := 0, 0
	complete := false
	response, err := ReadHTTPResponse(context.Background(), request, responseReader(t, data), HTTPResponseConfig{MaxInformational: 2, Information: func(status int, headers http.Header) error {
		interim++
		if status != 103 || headers.Get("Link") == "" {
			t.Fatal(status, headers)
		}
		headers.Set("X-Interim", "not final")
		return nil
	}, Body: BodyConfig{Close: func(done bool) error { closed++; complete = done; return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Request != request || response.ProtoMajor != 3 || response.Proto != "HTTP/3.0" || response.Status != "200 OK" || response.ContentLength != 3 || response.Body == nil {
		t.Fatal(response)
	}
	if len(response.Header.Values("Content-Length")) != 1 || response.Header.Get("Content-Length") != "3" || len(response.Header.Values("Set-Cookie")) != 2 || response.Header.Get("X-Interim") != "" || response.Header.Get("Trailer") != "" {
		t.Fatal(response.Header)
	}
	if value, ok := response.Trailer["X-Sum"]; !ok || value != nil {
		t.Fatal(response.Trailer)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "abc" {
		t.Fatal(string(body), err)
	}
	if response.Trailer.Get("X-Sum") != "ok" || response.Trailer.Get("X-Extra") != "separate" || response.Header.Get("X-Extra") != "" {
		t.Fatal(response.Trailer, response.Header)
	}
	if response.Body.Close() != nil || response.Body.Close() != nil || closed != 1 || !complete || interim != 1 {
		t.Fatal(closed, complete, interim)
	}
}
func TestHTTPResponseMetadataLength(t *testing.T) {
	for _, method := range []string{"HEAD", "CONNECT"} {
		t.Run(method, func(t *testing.T) {
			length := strings.Repeat("9", 40)
			if method == "CONNECT" {
				length = "ignored"
			}
			reader := responseReader(t, readerHeaders(t, fs(":status", "200"), fs("content-length", length)))
			reader.message.config.RequestMethod = method
			response, err := ReadHTTPResponse(context.Background(), &http.Request{Method: method}, reader, HTTPResponseConfig{Body: BodyConfig{Close: func(bool) error { return nil }}})
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.ContentLength != -1 {
				t.Fatal("overflow or tunnel length", response.ContentLength)
			}
			if method == "HEAD" && response.Header.Get("Content-Length") != length {
				t.Fatal("lost metadata")
			}
			if method == "CONNECT" && response.Header.Get("Content-Length") != "" {
				t.Fatal("tunnel length retained")
			}
			if _, err = io.ReadAll(response.Body); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestHTTPResponseFailureCleanup(t *testing.T) {
	for _, mode := range []string{"interim budget", "callback", "trailer declaration", "EOF"} {
		t.Run(mode, func(t *testing.T) {
			data := readerHeaders(t, fs(":status", "103"))
			maximum := 0
			callbackErr := errors.New("interim rejected")
			var callback func(int, http.Header) error
			switch mode {
			case "callback":
				maximum = 2
				callback = func(int, http.Header) error { return callbackErr }
			case "trailer declaration":
				data = readerHeaders(t, fs(":status", "200"), fs("trailer", "content-length"))
			case "EOF":
				data = nil
			}
			calls := 0
			response, err := ReadHTTPResponse(context.Background(), &http.Request{Method: "GET"}, responseReader(t, data), HTTPResponseConfig{MaxInformational: maximum, Information: callback, Body: BodyConfig{Close: func(complete bool) error {
				calls++
				if complete {
					t.Error("failure marked complete")
				}
				return nil
			}}})
			if err == nil || response != nil || calls != 1 {
				t.Fatal(response, err, calls)
			}
			if mode == "callback" && err != callbackErr {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTPResponseBodylessLength(t *testing.T) {
	for _, status := range []string{"204", "205", "304"} {
		response, err := ReadHTTPResponse(context.Background(), &http.Request{Method: "GET"}, responseReader(t, readerHeaders(t, fs(":status", status))), HTTPResponseConfig{Body: BodyConfig{Close: func(bool) error { return nil }}})
		if err != nil {
			t.Fatal(err)
		}
		if response.ContentLength != 0 {
			t.Fatal(status, response.ContentLength)
		}
		if body, err := io.ReadAll(response.Body); err != nil || len(body) != 0 {
			t.Fatal(status, body, err)
		}
		response.Body.Close()
	}
}
