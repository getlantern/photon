package transport

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestBorrowedUDPSocketShutdown(t *testing.T) {
	for _, shutdown := range []string{"close", "owner", "read-failure"} {
		t.Run(shutdown, func(t *testing.T) {
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			e, err := NewClientOnlyEndpoint(8, 4)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewUDPServerBorrowed(socket, e)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			switch shutdown {
			case "close":
				var wg sync.WaitGroup
				for i := 0; i < 8; i++ {
					wg.Go(func() {
						if err := runtime.Close(); err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
			case "owner":
				if err := runtime.Do(ctx, func(e *ServerEndpoint) error { e.Close(); return nil }); err != nil {
					t.Fatal(err)
				}
			case "read-failure":
				// Inject the error a socket deadline produces. Done must release the
				// borrowed socket even when the application never calls Close first.
				if err := socket.SetReadDeadline(time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-runtime.Done():
			case <-ctx.Done():
				t.Fatal("runtime did not stop")
			}
			select {
			case <-runtime.readerDone:
			default:
				t.Fatal("Done preceded reader exit")
			}
			if len(runtime.packets) != 0 {
				t.Fatal("queued buffers retained")
			}
			// Do not set a new deadline before writing: a leftover shutdown write
			// deadline would make this self-addressed datagram fail immediately.
			payload := []byte("caller regained socket")
			if _, err := socket.WriteToUDP(payload, socket.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal("borrowed socket unavailable", err)
			}
			received := make(chan error, 1)
			go func() {
				buffer := make([]byte, 128)
				n, _, err := socket.ReadFromUDP(buffer)
				if err == nil && !bytes.Equal(buffer[:n], payload) {
					err = errors.New("unexpected datagram after borrowed socket release")
				}
				received <- err
			}()
			select {
			case err := <-received:
				if err != nil {
					t.Fatal("socket read after release", err)
				}
			case <-ctx.Done():
				socket.Close()
				<-received
				t.Fatal("socket reader retained or deadline not cleared")
			}
		})
	}
}

func TestBorrowedRuntimeOwnsAddedSockets(t *testing.T) {
	primary, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	e, err := NewClientOnlyEndpoint(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewUDPServerBorrowed(primary, e)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	extra, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	if err := runtime.addSocket(context.Background(), extra); err != nil {
		t.Fatal(err)
	}
	if err := runtime.removeSocket(context.Background(), primary.LocalAddr().(*net.UDPAddr).AddrPort()); err == nil {
		t.Fatal("borrowed socket removed")
	}
	runtime.Close()
	if _, err := extra.WriteToUDP([]byte{1}, primary.LocalAddr().(*net.UDPAddr)); err == nil {
		t.Fatal("owned extra socket survived shutdown")
	}
	if _, err := primary.WriteToUDP([]byte{2}, primary.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal("borrowed primary closed", err)
	}
}

func TestBorrowedShutdownWithFullReceiveQueue(t *testing.T) {
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	e, err := NewClientOnlyEndpoint(8, 2)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewUDPServerBorrowed(socket, e)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	command := make(chan error, 1)
	go func() {
		command <- runtime.Do(context.Background(), func(*ServerEndpoint) error { close(entered); <-release; return nil })
	}()
	<-entered
	for i := 0; i < 128; i++ {
		if _, err := socket.WriteToUDP([]byte("queued during shutdown"), socket.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(runtime.packets) < cap(runtime.packets) {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("receive queue did not fill")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close() }()
	select {
	case <-runtime.readerDone:
	case <-time.After(time.Second):
		t.Fatal("full queue stranded reader")
	}
	unblock()
	if err := <-command; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("close stranded owner")
	}
	if len(runtime.packets) != 0 {
		t.Fatal("queued packet buffers retained")
	}
	if _, err := socket.WriteToUDP([]byte("released"), socket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
}
