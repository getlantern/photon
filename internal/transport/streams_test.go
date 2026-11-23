package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func streamParameters() wire.TransportParameters {
	p := wire.DefaultTransportParameters()
	p.InitialMaxData = 32768
	p.InitialMaxStreamDataBidiLocal = 8192
	p.InitialMaxStreamDataBidiRemote = 8192
	p.InitialMaxStreamDataUni = 8192
	p.InitialMaxStreamsBidi = 4
	p.InitialMaxStreamsUni = 4
	return p
}
func writeAll(t *testing.T, c *Conn, id uint64, data []byte) {
	t.Helper()
	for len(data) > 0 {
		n, err := c.WriteStream(id, data, true)
		if err != nil || n == 0 {
			t.Fatal(n, err)
		}
		data = data[n:]
	}
}
func TestEngineStreamTransfer(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			client, server := testPair(t, v, streamParameters())
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(uni)
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("independent QUIC stream\n"), 120)
			writeAll(t, client, id, payload)
			drivePair(t, client, server, true, true)
			accepted, err := server.AcceptStream(uni)
			if err != nil || accepted != id {
				t.Fatal(accepted, id, err)
			}
			dst := make([]byte, len(payload)+1)
			n, err := server.ReadStream(id, dst)
			if err != io.EOF || !bytes.Equal(dst[:n], payload) {
				t.Fatal("stream receive", n, err)
			}
			if uni {
				if _, err := server.WriteStream(id, []byte("illegal"), true); !errors.Is(err, stream.ErrStreamState) {
					t.Fatal(err)
				}
			} else {
				writeAll(t, server, id, payload)
				drivePair(t, client, server, true, true)
				n, err = client.ReadStream(id, dst)
				if err != io.EOF || !bytes.Equal(dst[:n], payload) {
					t.Fatal("stream response", n, err)
				}
			}
		}
	}
}
func TestEngineStreamFlowViolation(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	err := server.handleStreamFrame(wire.StreamFrame{StreamID: 0, Offset: 8192, Data: []byte("x")})
	var transport *TransportError
	if !errors.As(err, &transport) || transport.Code != 3 {
		t.Fatal(err)
	}
}
func TestEngineWriteCredit(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 8192)
	for len(data) > 0 {
		n, err := client.WriteStream(id, data, false)
		if err != nil || n == 0 {
			t.Fatal(n, err)
		}
		data = data[n:]
	}
	if _, err := client.WriteStream(id, []byte("x"), false); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal(err)
	}
	if n, err := client.WriteStream(id, nil, true); n != 0 || err != nil {
		t.Fatal("empty FIN at limit", n, err)
	}
}

func TestEngineStreamWindowUpdates(t *testing.T) {
	testEngineStreamWindowUpdates(t, 2048, 3072, 4096)
}

func TestEngineLargeStreamWindowUpdates(t *testing.T) {
	testEngineStreamWindowUpdates(t, 128<<10, 192<<10, 32768)
}

func testEngineStreamWindowUpdates(t *testing.T, streamWindow, connectionWindow uint64, repeats int) {
	t.Helper()
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			p := streamParameters()
			p.InitialMaxData = connectionWindow
			p.InitialMaxStreamDataBidiRemote = streamWindow
			client, server := testPair(t, version, p)
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("window turnover\n"), repeats)
			sent := 0
			var received []byte
			finished := false
			for round := 0; round < 200 && !finished; round++ {
				for sent < len(payload) {
					n, err := client.WriteStream(id, payload[sent:], true)
					if errors.Is(err, stream.ErrWouldBlock) {
						break
					}
					if err != nil || n == 0 {
						t.Fatal(n, err)
					}
					sent += n
				}
				drivePair(t, client, server, true, true)
				if round == 0 {
					got, err := server.AcceptStream(false)
					if err != nil || got != id {
						t.Fatal(got, err)
					}
				}
				for {
					dst := make([]byte, 333)
					n, err := server.ReadStream(id, dst)
					received = append(received, dst[:n]...)
					if err == io.EOF {
						finished = true
						break
					}
					if errors.Is(err, stream.ErrWouldBlock) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				drivePair(t, client, server, true, true)
			}
			if !finished || !bytes.Equal(payload, received) {
				t.Fatal("transfer stalled or corrupted", sent, len(received), len(payload))
			}
			if client.streams.send.Limit() <= p.InitialMaxData || client.streams.objects[id].send.Offset() != uint64(len(payload)) {
				t.Fatal("credit never advanced")
			}
		})
	}
}

func TestCreditUpdateSupersession(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	baseline := server.queuedBytes
	for i := uint64(1); i < 1000; i++ {
		if err := server.queueCredit(wire.MaxDataFrame{Maximum: 32768 + i}); err != nil {
			t.Fatal(err)
		}
		if err := server.queueCredit(wire.MaxStreamDataFrame{StreamID: 0, Maximum: 8192 + i}); err != nil {
			t.Fatal(err)
		}
	}
	if server.queuedBytes != baseline+64 || len(server.spaces[applicationSpace].queue) != 2 {
		t.Fatal("superseded credit retained", server.queuedBytes, len(server.spaces[applicationSpace].queue))
	}
}

func TestResetReplenishesConnectionCredit(t *testing.T) {
	p := streamParameters()
	p.InitialMaxData = 8192
	client, server := testPair(t, wire.Version1, p)
	drivePair(t, client, server, false, false)
	reset := wire.ResetStreamFrame{StreamID: 0, FinalSize: 8192, ErrorCode: 42}
	if err := server.handleStreamFrame(reset); err != nil {
		t.Fatal(err)
	}
	if server.streams.receive.Limit() != 16384 {
		t.Fatal("reset failed to replenish connection credit")
	}
	for i := 0; i < 10; i++ {
		if err := server.handleStreamFrame(reset); err != nil {
			t.Fatal(err)
		}
	}
	if server.streams.receive.Limit() != 16384 || server.queuedBytes != 32 {
		t.Fatal("duplicate reset replenished twice")
	}
	for _, item := range server.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.MaxStreamDataFrame); ok {
			t.Fatal("reset stream received more credit")
		}
	}
}
