package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestHandshakeAbsoluteDeadline(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := testPair(t, v)
		now := time.Unix(100, 0)
		if !c.HandshakeDeadline().IsZero() {
			t.Fatal("deadline armed without timestamp")
		}
		drain(t, c, now)
		deadline := now.Add(10 * time.Second)
		if c.HandshakeDeadline() != deadline {
			t.Fatal("default handshake limit")
		}
		c.HandleDatagram([]byte("garbage"), now.Add(time.Second))
		c.HandleDatagram(retryPacket(t, c, []byte("retrycid"), []byte{1}), now.Add(2*time.Second))
		drain(t, c, now.Add(2*time.Second))
		if c.HandshakeDeadline() != deadline {
			t.Fatal("traffic or Retry extended deadline")
		}
		if err := c.HandleTimeout(deadline); !errors.Is(err, ErrHandshakeTimeout) {
			t.Fatal(err)
		}
		if !c.closed || c.initialRead != nil || c.initialWrite != nil || c.HasOutstanding() || c.localIDs != nil || !c.RecoveryDeadline().IsZero() {
			t.Fatal("timeout retained resources")
		}
		if b, err := c.NextDatagram(deadline); len(b) != 0 || !errors.Is(err, ErrHandshakeTimeout) {
			t.Fatal("timeout output", err)
		}
	}
}

func TestHandshakeTimeoutBeforeLateInputOrOutput(t *testing.T) {
	for _, receive := range []bool{false, true} {
		c, s := testPair(t, wire.Version1)
		c.config.HandshakeTimeout = time.Second
		now := time.Unix(100, 0)
		drain(t, c, now)
		var err error
		if receive {
			err = c.HandleDatagram(makePeerPacket(t, s, initialSpace, 0, wire.PingFrame{}), now.Add(time.Second))
		} else {
			_, err = c.NextDatagram(now.Add(time.Second))
		}
		if !errors.Is(err, ErrHandshakeTimeout) || !c.closed {
			t.Fatal("late operation bypassed deadline", err)
		}
	}
}

func TestConfirmedConnectionHasNoHandshakeDeadline(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	if !c.HandshakeDeadline().IsZero() || !s.HandshakeDeadline().IsZero() {
		t.Fatal("confirmed handshake timer retained")
	}
	if err := c.HandleTimeout(time.Unix(1000, 0)); err != nil {
		t.Fatal(err)
	}
	config := c.config
	config.HandshakeTimeout = -time.Second
	if bad, err := NewConn(config); err == nil {
		bad.Close()
		t.Fatal("negative timeout accepted")
	}
}

func TestHandshakeIdleDeadline(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, v)
		c.config.HandshakeIdleTimeout = 3 * time.Second
		now := time.Unix(100, 0)
		drain(t, c, now)
		want := now.Add(3 * time.Second)
		if c.HandshakeDeadline() != want {
			t.Fatal("initial idle deadline", c.HandshakeDeadline())
		}
		if err := c.HandleDatagram([]byte("garbage"), now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if c.HandshakeDeadline() != want {
			t.Fatal("garbage moved deadline")
		}
		// Authenticated Initial activity restarts inactivity without completing TLS.
		if err := c.HandleDatagram(makePeerPacket(t, s, initialSpace, 0, wire.PingFrame{}), now.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
		want = now.Add(5 * time.Second)
		if c.HandshakeDeadline() != want {
			t.Fatal("authenticated receive did not restart idle", c.HandshakeDeadline())
		}
		// Existing send accounting permits only one ack-eliciting restart per receive.
		c.idle.sent(now.Add(3*time.Second), false)
		if c.HandshakeDeadline() != want {
			t.Fatal("ACK extended idle")
		}
		c.idle.sent(now.Add(3*time.Second), true)
		c.idle.sent(now.Add(4*time.Second), true)
		want = now.Add(6 * time.Second)
		if c.HandshakeDeadline() != want {
			t.Fatal("send rule", c.HandshakeDeadline())
		}
		if err := c.HandleTimeout(want); !errors.Is(err, ErrIdleTimeout) {
			t.Fatal("idle expiration", err)
		}
		if !c.closed || c.initialRead != nil || !c.RecoveryDeadline().IsZero() {
			t.Fatal("idle expiry retained resources")
		}
	}
}

func TestHandshakeIdleCannotExtendAbsoluteLimit(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	c.config.HandshakeIdleTimeout = 3 * time.Second
	now := time.Unix(100, 0)
	drain(t, c, now)
	for i := 1; i < 10; i++ {
		if err := c.HandleDatagram(makePeerPacket(t, s, initialSpace, uint64(i), wire.PingFrame{}), now.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if c.HandshakeDeadline() != now.Add(10*time.Second) {
		t.Fatal("absolute limit moved", c.HandshakeDeadline())
	}
	if err := c.HandleTimeout(now.Add(10 * time.Second)); !errors.Is(err, ErrHandshakeTimeout) {
		t.Fatal(err)
	}
}

func TestHandshakeIdleEndsAtTLSCompletion(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	now := time.Unix(100, 0)
	c.config.HandshakeIdleTimeout = time.Second
	drain(t, c, now)
	c.complete = true
	if !c.HandshakeDeadline().IsZero() {
		t.Fatal("completed TLS retained handshake limit")
	}
	c.confirmed = true
	if !c.HandshakeDeadline().IsZero() {
		t.Fatal("confirmed connection retained handshake limit")
	}
	cfg := c.config
	cfg.HandshakeIdleTimeout = -time.Second
	if bad, err := NewConn(cfg); err == nil {
		bad.Close()
		t.Fatal("negative handshake idle accepted")
	}
}

func TestHandshakeIdleBeforeLateOperation(t *testing.T) {
	for _, receive := range []bool{false, true} {
		c, s := testPair(t, wire.Version1)
		c.config.HandshakeIdleTimeout = time.Second
		now := time.Unix(100, 0)
		drain(t, c, now)
		var err error
		if receive {
			err = c.HandleDatagram(makePeerPacket(t, s, initialSpace, 0, wire.PingFrame{}), now.Add(time.Second))
		} else {
			_, err = c.NextDatagram(now.Add(time.Second))
		}
		if !errors.Is(err, ErrIdleTimeout) || !c.closed {
			t.Fatal("late operation bypassed idle timeout", err)
		}
	}
}

// Complete real TLS exchanges, then withhold the server's confirmation flight.
// A delayed HANDSHAKE_DONE must not turn a completed TLS handshake into a
// handshake timeout, nor suppress PTO or the ordinary negotiated idle timeout.
func TestTLSCompleteDelayedConfirmation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, resume := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/resume=%t", version, resume), func(t *testing.T) {
				params := wire.DefaultTransportParameters()
				params.MaxIdleTimeout = 30000
				c, s := testPair(t, version, params)
				now := time.Unix(100, 0)
				for _, b := range drain(t, c, now) {
					if err := s.HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
				for _, b := range drain(t, s, now) {
					if err := c.HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
				if !c.complete || c.confirmed {
					t.Fatal("expected TLS complete, QUIC unconfirmed")
				}
				for _, b := range drain(t, c, now) {
					if err := s.HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
				if !s.complete || !s.confirmed {
					t.Fatal("server did not complete handshake")
				}
				confirmation := drain(t, s, now)
				if len(confirmation) == 0 {
					t.Fatal("server produced no confirmation flight")
				}
				if !c.HandshakeDeadline().IsZero() {
					t.Fatal("TLS completion retained handshake deadline")
				}
				later := now.Add(11 * time.Second)
				if err := c.HandleTimeout(later); err != nil {
					t.Fatal("completed handshake expired", err)
				}
				if c.confirmed || c.spaces[handshakeSpace].discarded {
					t.Fatal("timeout changed confirmation or discarded handshake keys")
				}
				if probes := drain(t, c, later); len(probes) == 0 {
					t.Fatal("confirmation wait lost retransmission timer")
				}
				if resume {
					for _, b := range confirmation {
						if err := c.HandleDatagram(b, later); err != nil {
							t.Fatal("late confirmation", err)
						}
					}
					if !c.confirmed || !c.spaces[handshakeSpace].discarded {
						t.Fatal("late confirmation did not retire handshake keys")
					}
				} else {
					deadline := c.IdleDeadline()
					if deadline.IsZero() || !deadline.After(later) {
						t.Fatal("missing normal idle deadline", deadline)
					}
					if err := c.HandleTimeout(deadline); !errors.Is(err, ErrIdleTimeout) {
						t.Fatal("unconfirmed idle connection", err)
					}
					if !c.closed {
						t.Fatal("idle timeout retained connection")
					}
				}
			})
		}
	}
}
