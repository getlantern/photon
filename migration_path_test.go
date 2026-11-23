package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func migrationTransport(t *testing.T) *Transport {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	tr := &Transport{Conn: socket}
	t.Cleanup(func() { tr.Close(); socket.Close() })
	return tr
}

func TestPublicAddPathProbeSwitch(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, mp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/multipath=%v", v, mp), func(t *testing.T) {
				ct, st := dialTLS(t)
				cfg := &Config{Versions: []Version{v}, EnableMultipath: mp}
				listener, err := ListenAddr("127.0.0.1:0", st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				primary, alternative := migrationTransport(t), migrationTransport(t)
				client, err := primary.Dial(ctx, listener.Addr(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				old := client.LocalAddr().String()
				if _, err := server.AddPath(alternative); err == nil {
					t.Fatal("server initiated migration")
				}
				if _, err := client.AddPath(primary); err == nil {
					t.Fatal("primary socket added as alternative")
				}
				path, err := client.AddPath(alternative)
				if err != nil {
					t.Fatal(err)
				}
				if client.LocalAddr().String() != old || path.LocalAddr().String() != alternative.Conn.LocalAddr().String() {
					t.Fatal("AddPath moved binding")
				}
				if err := path.Switch(); !errors.Is(err, ErrPathNotValidated) {
					t.Fatal("unvalidated switch", err)
				}
				canceled, stop := context.WithCancel(ctx)
				stop()
				if err := path.Probe(canceled); !errors.Is(err, context.Canceled) {
					t.Fatal("canceled probe", err)
				}
				for range 2 {
					if err := path.Probe(ctx); err != nil {
						t.Fatal("probe", err)
					}
				}
				if client.LocalAddr().String() != old {
					t.Fatal("probe moved ordinary traffic")
				}
				exchange := func(label string) {
					t.Helper()
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte(label)); err != nil {
						t.Fatal(err)
					}
					stream.Close()
					received, err := server.AcceptStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					received.SetReadDeadline(time.Now().Add(time.Second))
					data, err := io.ReadAll(received)
					if err != nil || string(data) != label {
						t.Fatal("payload", string(data), err)
					}
					if _, err := received.Write([]byte("reply")); err != nil {
						t.Fatal(err)
					}
					received.Close()
					stream.SetReadDeadline(time.Now().Add(time.Second))
					data, err = io.ReadAll(stream)
					if err != nil || string(data) != "reply" {
						t.Fatal("reply", string(data), err)
					}
				}
				exchange("before switch")
				if err := path.Switch(); err != nil {
					t.Fatal(err)
				}
				if err := path.Switch(); err != nil {
					t.Fatal("repeat switch", err)
				}
				if client.LocalAddr().String() != alternative.Conn.LocalAddr().String() {
					t.Fatal("switch retained original socket")
				}
				exchange("after switch")
				if err := path.Close(); err == nil {
					t.Fatal("closed active path")
				}
				// Exercise CID replenishment beyond the initial supply using our peer.
				for round := range 5 {
					next, err := client.AddPath(migrationTransport(t))
					if err != nil {
						t.Fatal("add repeated path", round, err)
					}
					if err := next.Probe(ctx); err != nil {
						t.Fatal("probe repeated path", round, err)
					}
					exchange("before repeated switch")
					if err := next.Switch(); err != nil {
						t.Fatal("repeated switch", round, err)
					}
					exchange("after repeated switch")
					if err := path.Close(); err != nil {
						t.Fatal("close replaced path", round, err)
					}
					path = next
				}
				client.CloseWithError(0, "done")
			})
		}
	}
}

func TestPublicAddPathCloseBeforeProbe(t *testing.T) {
	ct, st := dialTLS(t)
	listener, err := ListenAddr("127.0.0.1:0", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	primary, alternative := migrationTransport(t), migrationTransport(t)
	c, err := primary.Dial(ctx, listener.Addr(), ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.AddPath(alternative)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal("repeat close", err)
	}
	if err := p.Probe(ctx); !errors.Is(err, ErrPathClosed) {
		t.Fatal("closed probe", err)
	}
	if err := p.Switch(); !errors.Is(err, ErrPathClosed) {
		t.Fatal("closed switch", err)
	}
	if _, err := c.AddPath(alternative); err != nil {
		t.Fatal("closed association could not be replaced", err)
	}
	alternative.Close()
	// Transport closure leaves the caller's PacketConn open.
	if err := alternative.Conn.SetReadDeadline(time.Now()); err != nil {
		t.Fatal("path/transport closed caller socket", err)
	}
}

// Drop candidate output while reporting successful writes, so cancellation and
// closure happen after a real probe is sealed without depending on network timing.
type migrationDropSocket struct {
	net.PacketConn
	drop   atomic.Bool
	writes chan struct{}
}

func (s *migrationDropSocket) WriteTo(b []byte, addr net.Addr) (int, error) {
	if s.drop.Load() {
		select {
		case s.writes <- struct{}{}:
		default:
		}
		return len(b), nil
	}
	return s.PacketConn.WriteTo(b, addr)
}

func TestPublicAddPathPendingProbeLifecycle(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, action := range []string{"cancel-retry", "path-close", "source-close", "owner-close"} {
			t.Run(fmt.Sprintf("%v/%s", version, action), func(t *testing.T) {
				ct, st := dialTLS(t)
				cfg := &Config{Versions: []Version{version}}
				listener, err := ListenAddr("127.0.0.1:0", st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				primary, alternative := migrationTransport(t), migrationTransport(t)
				drop := &migrationDropSocket{PacketConn: alternative.Conn, writes: make(chan struct{}, 1)}
				drop.drop.Store(true)
				alternative.Conn = drop
				c, err := primary.Dial(ctx, listener.Addr(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				p, err := c.AddPath(alternative)
				if err != nil {
					t.Fatal(err)
				}
				old := c.LocalAddr().String()
				probeCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- p.Probe(probeCtx) }()
				select {
				case <-drop.writes:
				case <-ctx.Done():
					t.Fatal("probe not emitted", ctx.Err())
				}
				// A second call must be cancellable even while the first owns the gate.
				waiting, cancelWaiting := context.WithCancel(ctx)
				second := make(chan error, 1)
				go func() { second <- p.Probe(waiting) }()
				cancelWaiting()
				select {
				case err := <-second:
					if !errors.Is(err, context.Canceled) {
						t.Fatal("waiting probe", err)
					}
				case <-ctx.Done():
					t.Fatal("waiting probe stuck")
				}
				want := ErrPathClosed
				switch action {
				case "cancel-retry":
					cancel()
					want = context.Canceled
				case "path-close":
					if err := p.Close(); err != nil {
						t.Fatal(err)
					}
				case "source-close":
					if err := alternative.Close(); err != nil {
						t.Fatal(err)
					}
				case "owner-close":
					if err := primary.Close(); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case err := <-result:
					if !errors.Is(err, want) {
						t.Fatal("pending probe", err, "want", want)
					}
				case <-ctx.Done():
					t.Fatal("probe did not finish", ctx.Err())
				}
				if c.LocalAddr().String() != old {
					t.Fatal("failed probe switched binding")
				}
				if action != "owner-close" {
					server, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx, err := c.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal("primary stream", err)
					}
					if _, err := tx.Write([]byte("primary survives")); err != nil {
						t.Fatal(err)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					rx, err := server.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					data, err := io.ReadAll(rx)
					if err != nil || string(data) != "primary survives" {
						t.Fatal("primary transfer", string(data), err)
					}
				}
				if action == "cancel-retry" {
					drop.drop.Store(false)
					if err := p.Probe(ctx); err != nil {
						t.Fatal("retry", err)
					}
					if err := p.Switch(); err != nil {
						t.Fatal("switch after retry", err)
					}
				}
			})
		}
	}
}

// A candidate Transport may already carry another connection. Its sole reader
// must demultiplex both its own connection and the migrated connection's CIDs.
func TestPublicAddPathSharedSource(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			cfg := &Config{Versions: []Version{version}}
			listener, err := ListenAddr("127.0.0.1:0", st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			primary, shared := migrationTransport(t), migrationTransport(t)
			migrating, err := primary.Dial(ctx, listener.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer migrating.CloseWithError(0, "")
			migratedPeer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			resident, err := shared.Dial(ctx, listener.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer resident.CloseWithError(0, "")
			residentPeer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			path, err := migrating.AddPath(shared)
			if err != nil {
				t.Fatal(err)
			}
			if err := path.Probe(ctx); err != nil {
				t.Fatal(err)
			}
			if err := path.Switch(); err != nil {
				t.Fatal(err)
			}
			if migrating.LocalAddr().String() != resident.LocalAddr().String() {
				t.Fatal("connections do not share socket")
			}
			exchange := func(client, server *Conn, payload string) {
				t.Helper()
				tx, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Write([]byte(payload)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				data, err := io.ReadAll(rx)
				if err != nil || string(data) != payload {
					t.Fatal("request", string(data), err)
				}
				if _, err := rx.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := rx.Close(); err != nil {
					t.Fatal(err)
				}
				tx.SetReadDeadline(time.Now().Add(time.Second))
				data, err = io.ReadAll(tx)
				if err != nil || string(data) != payload {
					t.Fatal("reply", string(data), err)
				}
			}
			for round := range 3 {
				exchange(migrating, migratedPeer, fmt.Sprint("migrated-", round))
				exchange(resident, residentPeer, fmt.Sprint("resident-", round))
			}
			if err := migrating.CloseWithError(0, "finished"); err != nil {
				t.Fatal(err)
			}
			exchange(resident, residentPeer, "resident-after-migrated-close")
		})
	}
}
