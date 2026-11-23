package http3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestReplayRequest(t *testing.T) {
	request, _ := http.NewRequest("POST", "https://example.com", strings.NewReader("payload"))
	replay, err := replayRequest(request)
	if err != nil || replay == nil || replay == request || replay.Body == request.Body {
		t.Fatal(replay, err)
	}
	payload, err := io.ReadAll(replay.Body)
	replay.Body.Close()
	request.Body.Close()
	if err != nil || string(payload) != "payload" {
		t.Fatal(string(payload), err)
	}
	request.Trailer = http.Header{"X-Final": nil}
	if replay, err := replayRequest(request); replay != nil || err != nil {
		t.Fatal("trailer upload replayed", replay, err)
	}
	request.Trailer = nil
	request.GetBody = nil
	if replay, err := replayRequest(request); replay != nil || err != nil {
		t.Fatal("unrepeatable body replayed", replay, err)
	}
	sentinel := errors.New("GetBody failed")
	body := &poolTestBody{Reader: strings.NewReader("unused")}
	request.GetBody = func() (io.ReadCloser, error) { return body, sentinel }
	if _, err := replayRequest(request); !errors.Is(err, sentinel) || body.closed.Load() != 1 {
		t.Fatal(err, body.closed.Load())
	}
	request.GetBody = func() (io.ReadCloser, error) { return nil, nil }
	if _, err := replayRequest(request); err == nil {
		t.Fatal("nil replay body accepted")
	}
	request.Body = http.NoBody
	if replay, err := replayRequest(request); err != nil || replay == nil || replay.Body != http.NoBody {
		t.Fatal(replay, err)
	}
}

func TestReplayRequestCancellation(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(fmt.Sprintf("during=%v", during), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			sentinel := errors.New("replay canceled")
			request, _ := http.NewRequestWithContext(ctx, "POST", "https://example.com", strings.NewReader("original"))
			defer request.Body.Close()
			replacement := &poolTestBody{Reader: strings.NewReader("replacement")}
			calls := 0
			request.GetBody = func() (io.ReadCloser, error) {
				calls++
				cancel(sentinel)
				return replacement, errors.New("secondary reconstruction failure")
			}
			if !during {
				cancel(sentinel)
			}
			replay, err := replayRequest(request)
			if replay != nil || !errors.Is(err, sentinel) {
				t.Fatal(replay, err)
			}
			want := int32(0)
			if during {
				want = 1
			}
			if calls != int(want) || replacement.closed.Load() != want {
				t.Fatal("canceled replay ownership", calls, replacement.closed.Load())
			}
		})
	}
}
