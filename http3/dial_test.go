package http3

import (
	"context"
	"errors"
	"testing"
)

func TestDialRejectsInvalidInputBeforeNetwork(t *testing.T) {
	for _, origin := range []string{"http://example.com", "https://example.com/path", "https://user:pass@example.com", "https://example.com/?q=1", "https://example.com/#fragment", "https://:443", "https://example.com:99999"} {
		if client, err := Dial(context.Background(), origin, nil, nil, nil); err == nil || client != nil {
			if client != nil {
				client.Close()
			}
			t.Fatal(origin, err)
		}
	}
	if client, err := Dial(context.Background(), "https://example.com", nil, nil, &ClientConfig{MaxConcurrentRequests: -1}); err == nil || client != nil {
		t.Fatal(client, err)
	}
	if client, err := Dial(nil, "https://example.com", nil, nil, nil); err == nil || client != nil {
		t.Fatal(client, err)
	}
}
func TestDialPreservesCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("setup abandoned")
	cancel(cause)
	if client, err := Dial(ctx, "https://example.com", nil, nil, nil); client != nil || !errors.Is(err, cause) {
		t.Fatal(client, err)
	}
}
