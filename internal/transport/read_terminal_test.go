package transport

import (
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveTerminalCauseOrdering(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, first := range []string{"eof", "reset", "local"} {
			t.Run(vName(v)+"/"+first, func(t *testing.T) {
				client, server := testPair(t, v, streamParameters())
				drivePair(t, client, server, false, false)
				id, err := client.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := client.WriteStream(id, []byte("x"), first == "eof"); err != nil {
					t.Fatal(err)
				}
				drivePair(t, client, server, false, false)
				if _, err := server.AcceptStream(false); err != nil {
					t.Fatal(err)
				}
				switch first {
				case "eof":
					n, err := server.ReadStream(id, make([]byte, 1))
					if n != 1 || err != io.EOF {
						t.Fatal(n, err)
					}
				case "reset":
					if err := server.handleStreamFrame(wire.ResetStreamFrame{StreamID: id, ErrorCode: 12, FinalSize: 1}); err != nil {
						t.Fatal(err)
					}
				}
				if err := server.CancelRead(id, 9); err != nil {
					t.Fatal(err)
				}
				if err := server.handleStreamFrame(wire.ResetStreamFrame{StreamID: id, ErrorCode: 12, FinalSize: 1}); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					n, err := server.ReadStream(id, make([]byte, 1))
					if n != 0 {
						t.Fatal(n)
					}
					if first == "eof" {
						if err != io.EOF {
							t.Fatal("EOF replaced", err)
						}
						continue
					}
					var stopped *StreamError
					code := StreamErrorCode(9)
					if first == "reset" {
						code = 12
					}
					if !errors.As(err, &stopped) || stopped.StreamID != StreamID(id) || stopped.ErrorCode != code || stopped.Remote != (first == "reset") {
						t.Fatal("first cause lost", err)
					}
				}
			})
		}
	}
}
