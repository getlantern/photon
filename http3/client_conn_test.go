package http3

import (
	"net/url"
	"testing"
)

func TestClientOriginCanonicalization(t *testing.T) {
	for _, text := range []string{"https://Example.COM", "https://example.com:443", "https://EXAMPLE.com:0443"} {
		target, _ := url.Parse(text)
		key, err := originKey(target)
		if err != nil || key != "example.com:443" {
			t.Fatal(text, key, err)
		}
	}
	for _, text := range []string{"http://example.com", "https://:443", "https://example.com:65536", "https://é.example", "https:opaque"} {
		target, _ := url.Parse(text)
		if key, err := originKey(target); err == nil {
			t.Fatal("invalid origin", text, key)
		}
	}
	target, _ := url.Parse("https://[::1]:8443/")
	if key, err := originKey(target); err != nil || key != "[::1]:8443" {
		t.Fatal(key, err)
	}
}
