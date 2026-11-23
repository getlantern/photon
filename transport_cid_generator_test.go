package quic

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type failingCIDGenerator struct {
	length int
	err    error
}

func (g failingCIDGenerator) ConnectionIDLen() int { return g.length }
func (g failingCIDGenerator) GenerateConnectionID() (ConnectionID, error) {
	return ConnectionIDFromBytes([]byte("bad")), g.err
}

func TestTransportCIDGeneratorFailures(t *testing.T) {
	for _, length := range []int{-1, 21} {
		tr := &Transport{ConnectionIDGenerator: failingCIDGenerator{length: length}}
		if _, err := tr.init(1); err == nil || tr.runtime != nil {
			t.Fatal("bad generator length accepted")
		}
	}
	tr := &Transport{ConnectionIDGenerator: &testCIDGenerator{}, StatelessResetKey: &StatelessResetKey{1}}
	if _, err := tr.init(1); err == nil || tr.runtime != nil {
		t.Fatal("zero CID with reset key")
	}
	ct, st := dialTLS(t)
	for _, failure := range []error{nil, errors.New("custom generator failed")} {
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer socket.Close()
		tr := &Transport{Conn: socket, ConnectionIDGenerator: failingCIDGenerator{length: 4, err: failure}}
		defer tr.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = tr.Dial(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, ct, nil)
		cancel()
		if err == nil || failure != nil && !errors.Is(err, failure) {
			t.Fatal("generator failure lost", err)
		}
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	zero := &Transport{Conn: socket, ConnectionIDGenerator: &testCIDGenerator{}}
	defer zero.Close()
	if _, err := zero.Listen(st, &Config{EnableMultipath: true}); err == nil {
		t.Fatal("zero CID multipath listener")
	}
}

func TestTransportCIDGeneratorConcurrentDial(t *testing.T) {
	ct, st := dialTLS(t)
	listener, err := ListenAddr("127.0.0.1:0", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	tr := &Transport{Conn: socket, ConnectionIDGenerator: &testCIDGenerator{length: 8}}
	defer tr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for range 8 {
		workers.Go(func() { _, err := tr.Dial(ctx, listener.Addr(), ct, nil); failures <- err })
	}
	workers.Wait()
	for range 8 {
		if err := <-failures; err != nil {
			t.Fatal(err)
		}
	}
	for range 8 {
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
}
