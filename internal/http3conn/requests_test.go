package http3conn

import (
	"context"
	"errors"
	"sync"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func requestSession(t *testing.T, limit int) *Session {
	t.Helper()
	transport := &dispatchTransport{streams: make(chan ReceiveStream)}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
	session, err := StartSession(context.Background(), transport, local, SessionConfig{PeerIsServer: true, MaxRequests: limit, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Stop(context.Canceled) })
	return session
}
func TestRequestAdmissionAndGoaway(t *testing.T) {
	s := requestSession(t, 2)
	if _, err := s.BeginRequest(1); err != ErrRequestID {
		t.Fatal(err)
	}
	a, err := s.BeginRequest(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.BeginRequest(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.BeginRequest(0); err != ErrRequestID {
		t.Fatal(err)
	}
	if _, err = s.BeginRequest(8); err != ErrRequestLimit {
		t.Fatal(err)
	}
	a.Finish()
	a.Finish()
	c, err := s.BeginRequest(8)
	if err != nil {
		t.Fatal(err)
	}
	s.receiveGoaway(PeerGoaway{ID: 8, RequestIDs: true})
	if context.Cause(c.Context()) != ErrRequestRejected || b.Context().Err() != nil {
		t.Fatal("wrong rejection boundary")
	}
	if _, err = s.BeginRequest(12); err != ErrPeerDraining {
		t.Fatal(err)
	}
	// Even a lower, previously unused stream ID cannot be initiated after GOAWAY.
	if _, err = s.BeginRequest(0); err != ErrPeerDraining {
		t.Fatal(err)
	}
	s.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
	if context.Cause(b.Context()) != ErrRequestRejected {
		t.Fatal("lower limit not applied")
	}
	s.requestsMu.Lock()
	retained := len(s.requests)
	s.requestsMu.Unlock()
	if retained != 2 {
		t.Fatal("rejected owners escaped budget")
	}
	b.Finish()
	c.Finish()
	s.requestsMu.Lock()
	retained = len(s.requests)
	s.requestsMu.Unlock()
	if retained != 0 {
		t.Fatal("lease leak")
	}
}
func TestRequestAdmissionConnectionFailure(t *testing.T) {
	s := requestSession(t, 1)
	lease, err := s.BeginRequest(0)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("connection failed")
	s.Stop(cause)
	if context.Cause(lease.Context()) != cause {
		t.Fatal("failure became retry assurance")
	}
	if _, err := s.BeginRequest(4); err != cause {
		t.Fatal(err)
	}
	lease.Finish()
	if _, err := requestSession(t, 0).BeginRequest(0); err != ErrRequestLimit {
		t.Fatal(err)
	}
}
func TestRequestAdmissionRacesGoaway(t *testing.T) {
	for i := 0; i < 64; i++ {
		s := requestSession(t, 1)
		var lease *RequestLease
		var err error
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; lease, err = s.BeginRequest(0) }()
		go func() { defer wg.Done(); <-start; s.receiveGoaway(PeerGoaway{ID: 0, RequestIDs: true}) }()
		close(start)
		wg.Wait()
		if err != nil {
			if err != ErrPeerDraining {
				t.Fatal(err)
			}
		} else {
			if context.Cause(lease.Context()) != ErrRequestRejected {
				t.Fatal("request escaped GOAWAY")
			}
			lease.Finish()
		}
		s.Stop(context.Canceled)
	}
}

func TestGoawayNotificationBeforeAndAfterReceipt(t *testing.T) {
	for _, subscribeFirst := range []bool{false, true} {
		session := requestSession(t, 1)
		var signal <-chan struct{}
		if subscribeFirst {
			signal = session.GoawayReceived()
			select {
			case <-signal:
				t.Fatal("premature notification")
			default:
			}
		}
		session.receiveGoaway(PeerGoaway{ID: 8, RequestIDs: true})
		if !subscribeFirst {
			signal = session.GoawayReceived()
		}
		select {
		case <-signal:
		default:
			t.Fatal("missing GOAWAY notification")
		}
		session.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
		if session.GoawayReceived() != signal {
			t.Fatal("notification channel replaced")
		}
	}
}
