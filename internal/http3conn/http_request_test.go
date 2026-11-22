package http3conn

import (
	"context"
	"io"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func requestReader(t *testing.T, data []byte, extended bool) *MessageReader {
	t.Helper()
	r := responseReader(t, data)
	message, err := NewMessage(MessageConfig{Kind: RequestFields, ExtendedConnect: extended})
	if err != nil {
		t.Fatal(err)
	}
	r.message = message
	return r
}
func requestFields(path string) []qpack.DecodedField {
	return []qpack.DecodedField{fs(":method", "POST"), fs(":scheme", "https"), fs(":authority", "example.com:443"), fs(":path", path)}
}
func TestHTTPRequestAdapter(t *testing.T) {
	fields := append(requestFields("/a%2Fb?q=x%2Fy"), fs("content-length", "02, 2"), fs("cookie", "a=1"), fs("cookie", "b=2"), fs("trailer", "x-checksum"))
	data := readerHeaders(t, fields...)
	data = append(data, readerFrame(t, h3.Data, []byte("ok"))...)
	data = append(data, readerHeaders(t, fs("x-checksum", "done"))...)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closed := 0
	request, control, err := ReadHTTPRequest(ctx, requestReader(t, data, false), BodyConfig{Close: func(complete bool) error {
		closed++
		if !complete {
			t.Error("message not complete")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if request.Context() != ctx || request.Method != "POST" || request.ProtoMajor != 3 || request.Host != "example.com:443" || request.Header.Get("Host") != "" || request.URL.Scheme != "" || request.URL.Host != "" || request.URL.Path != "/a/b" || request.URL.RawPath != "/a%2Fb" || request.URL.RawQuery != "q=x%2Fy" || request.RequestURI != "/a%2Fb?q=x%2Fy" || control.Protocol != "" || control.Scheme != "https" || control.Authority != "example.com:443" {
		t.Fatal(request, control)
	}
	if request.Header.Get("Cookie") != "a=1; b=2" || request.ContentLength != 2 {
		t.Fatal(request.Header, request.ContentLength)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil || string(body) != "ok" || request.Trailer.Get("X-Checksum") != "done" || request.Header.Get("X-Checksum") != "" {
		t.Fatal(string(body), err, request.Trailer)
	}
	request.Body.Close()
	request.Body.Close()
	if closed != 1 {
		t.Fatal(closed)
	}
}
func TestHTTPRequestCONNECT(t *testing.T) {
	for _, extended := range []bool{false, true} {
		fields := []qpack.DecodedField{fs(":method", "CONNECT"), fs(":authority", "example.com:443")}
		if extended {
			fields = append(fields, fs(":scheme", "https"), fs(":path", "/chat"), fs(":protocol", "websocket"))
		}
		request, control, err := ReadHTTPRequest(context.Background(), requestReader(t, readerHeaders(t, fields...), extended), BodyConfig{Close: func(bool) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		if request.Host != "example.com:443" || request.URL.Scheme != "" || request.ContentLength != -1 {
			t.Fatal(request)
		}
		if extended {
			if control.Protocol != "websocket" || control.Scheme != "https" || request.URL.Path != "/chat" || request.RequestURI != "/chat" || request.URL.Host != "" {
				t.Fatal(control, request.URL)
			}
		} else if request.RequestURI != "example.com:443" || request.URL.Path != "" || request.URL.Host != "example.com:443" {
			t.Fatal(request)
		}
		request.Body.Close()
	}
}
func TestHTTPRequestBadTargetCleanup(t *testing.T) {
	for _, target := range []string{"/bad%zz", "/a#fragment"} {
		calls := 0
		request, _, err := ReadHTTPRequest(context.Background(), requestReader(t, readerHeaders(t, requestFields(target)...), false), BodyConfig{Close: func(bool) error { calls++; return nil }})
		if request != nil || err == nil || calls != 1 {
			t.Fatal(request, err, calls)
		}
	}
}

func TestHTTPRequestAuthorityAndAsterisk(t *testing.T) {
	for _, authority := range []string{"bad host", "example.com/path", "example.com?query", "[::1"} {
		fields := requestFields("/")
		fields[2].Value = authority
		request, _, err := ReadHTTPRequest(context.Background(), requestReader(t, readerHeaders(t, fields...), false), BodyConfig{Close: func(bool) error { return nil }})
		if err == nil || request != nil {
			t.Fatal("invalid authority accepted", authority)
		}
	}
	fields := []qpack.DecodedField{fs(":method", "OPTIONS"), fs(":scheme", "https"), fs(":path", "*"), fs("host", "example.com")}
	request, _, err := ReadHTTPRequest(context.Background(), requestReader(t, readerHeaders(t, fields...), false), BodyConfig{Close: func(bool) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer request.Body.Close()
	if request.URL.Scheme != "" || request.URL.Host != "" || request.RequestURI != "*" || request.URL.Path != "*" || request.Host != "example.com" || request.Header.Get("Host") != "" {
		t.Fatal(request)
	}
}
