package http3conn

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestHTTPRequestHeadersRoundTrip(t *testing.T) {
	request, err := http.NewRequest("POST", "https://user:secret@route.example/a%2Fb?q=x%2Fy#local", strings.NewReader("abc"))
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "origin.example:443"
	request.Header = http.Header{"Cookie": {"a=1", "b=2"}, "X-Test": {"one", "two"}, "Host": {"ignored"}, "Content-Length": {"99"}}
	request.Trailer = http.Header{"X-Checksum": nil}
	before := request.Clone(context.Background())
	fields, err := HTTPRequestHeaders(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.Header, before.Header) || !reflect.DeepEqual(request.URL, before.URL) || request.Method != before.Method {
		t.Fatal("request mutated")
	}
	for _, field := range fields {
		if (field.Name == "authorization" || field.Name == "cookie") && !field.NeverIndex {
			t.Fatal("sensitive indexing", field)
		}
	}
	wire := readerHeaders(t, fields...)
	wire = append(wire, readerFrame(t, 0, []byte("abc"))...)
	wire = append(wire, readerHeaders(t, fs("x-checksum", "ok"))...)
	reader := requestReader(t, wire, false)
	received, _, err := ReadHTTPRequest(context.Background(), reader, BodyConfig{Close: func(bool) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer received.Body.Close()
	body, err := io.ReadAll(received.Body)
	if err != nil || string(body) != "abc" || received.RequestURI != "/a%2Fb?q=x%2Fy" || received.Host != "origin.example:443" || received.Header.Get("Cookie") != "a=1; b=2" || received.ContentLength != 3 || received.Trailer.Get("X-Checksum") != "ok" {
		t.Fatal(received, string(body), err)
	}
	user, password, ok := received.BasicAuth()
	if !ok || user != "user" || password != "secret" {
		t.Fatal("userinfo authorization", user, ok)
	}
}
func TestHTTPRequestHeadersLengthsAndTargets(t *testing.T) {
	for _, tc := range []struct {
		method, path         string
		body                 io.ReadCloser
		length               int64
		wantLength, wantPath string
	}{
		{"", "", nil, 0, "", "/"},
		{"POST", "/", nil, 0, "0", "/"},
		{"PUT", "/", http.NoBody, 0, "0", "/"},
		{"POST", "/", io.NopCloser(strings.NewReader("abc")), 0, "", "/"},
		{"POST", "/", io.NopCloser(strings.NewReader("abc")), -1, "", "/"},
		{"POST", "/", io.NopCloser(strings.NewReader("abc")), 3, "3", "/"},
		{"OPTIONS", "*", nil, 0, "", "*"},
		{"CONNECT", "ignored", nil, 0, "", ""},
	} {
		request := &http.Request{Method: tc.method, URL: &url.URL{Scheme: "https", Host: "example.com:443", Path: tc.path}, Body: tc.body, ContentLength: tc.length}
		fields, err := HTTPRequestHeaders(request)
		if err != nil {
			t.Fatal(tc, err)
		}
		gotLength, gotPath := "", ""
		for _, field := range fields {
			switch field.Name {
			case "content-length":
				gotLength = field.Value
			case ":path":
				gotPath = field.Value
			}
		}
		if gotLength != tc.wantLength || gotPath != tc.wantPath {
			t.Fatal(tc, gotLength, gotPath)
		}
	}
}
func TestHTTPRequestHeadersRejectMalformed(t *testing.T) {
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.RequestURI = "/override" },
		func(r *http.Request) { r.URL.Opaque = "//other/" },
		func(r *http.Request) { r.Host = "user@host" },
		func(r *http.Request) { r.Host = "host/path" },
		func(r *http.Request) { r.Host = "host\r\nx: y" },
		func(r *http.Request) { r.Host = "é.example" },
		func(r *http.Request) { r.URL.Scheme = "http" },
		func(r *http.Request) { r.URL.RawQuery = "a=#fragment" },
		func(r *http.Request) { r.Method = "GET\n" },
		func(r *http.Request) { r.ContentLength = 4 },
		func(r *http.Request) { r.ContentLength = -2 },
		func(r *http.Request) { r.Header.Set("Connection", "keep-alive") },
		func(r *http.Request) { r.Header.Set("X-Test", "a\r\nb") },
		func(r *http.Request) { r.Header.Set(":path", "/other") },
		func(r *http.Request) { r.TransferEncoding = []string{"gzip"} },
		func(r *http.Request) { r.Trailer = http.Header{"Content-Length": nil} },
		func(r *http.Request) { r.Trailer = http.Header{"X-One, X-Two": nil} },
	} {
		request, _ := http.NewRequest("GET", "https://example.com/", nil)
		mutate(request)
		if fields, err := HTTPRequestHeaders(request); err == nil {
			t.Fatal("malformed request accepted", request, fields)
		}
	}
}
func TestHTTPRequestHeadersAuthorizationAndDeterminism(t *testing.T) {
	request, _ := http.NewRequest("GET", "https://user:pass@example.com/?", nil)
	request.Header.Set("Authorization", "Bearer existing")
	request.Trailer = http.Header{"X-Z": nil, "X-A": nil}
	a, err := HTTPRequestHeaders(request)
	if err != nil {
		t.Fatal(err)
	}
	b, err := HTTPRequestHeaders(request)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("nondeterministic fields", err)
	}
	auth := 0
	for _, field := range a {
		if field.Name == "authorization" {
			auth++
			if field.Value != "Bearer existing" {
				t.Fatal(field)
			}
		}
		if field.Name == ":path" && field.Value != "/?" {
			t.Fatal(field)
		}
		if field.Name == "trailer" && field.Value != "x-a, x-z" {
			t.Fatal(field)
		}
	}
	if auth != 1 {
		t.Fatal(auth)
	}
	// Conversion must not consume the upload.
	request.Body = io.NopCloser(bytes.NewBufferString("unread"))
	if _, err := HTTPRequestHeaders(request); err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(request.Body)
	if string(body) != "unread" {
		t.Fatal(string(body))
	}
}
