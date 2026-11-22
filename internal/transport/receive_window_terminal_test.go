package transport

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveWindowTerminalAccounting(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"read-fin", "cancel-fin", "cancel-open", "reset", "reset-gap"} {
			t.Run(fmt.Sprintf("%s/%s", vName(version), mode), func(t *testing.T) {
				p := streamParameters()
				p.InitialMaxData = 3072
				p.InitialMaxStreamDataBidiRemote = 2048
				client, server := testPair(t, version, p)
				server.config.MaxStreamReceiveWindow = 4096
				server.config.MaxConnectionReceiveWindow = 6144
				drivePair(t, client, server, false, false)
				id, err := client.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				remaining := 2048
				if mode == "reset-gap" {
					remaining = 1
				}
				fin := mode == "read-fin" || mode == "cancel-fin"
				for remaining > 0 {
					n, err := client.WriteStream(id, make([]byte, remaining), fin)
					if err != nil || n == 0 {
						t.Fatal(n, err)
					}
					remaining -= n
				}
				drivePair(t, client, server, false, false)
				server.recovery.RTT.Sampled = true
				server.recovery.RTT.Smoothed = 20 * time.Millisecond
				state := server.streams.objects[id]
				now := time.Unix(100, 0)
				if n, err := server.ReadStreamAt(id, make([]byte, 1), now); n != 1 || err != nil {
					t.Fatal(n, err)
				}
				sample := server.streams.windowSample
				switch mode {
				case "read-fin":
					for i, size := range []int{1536, 511} {
						n, err := server.ReadStreamAt(id, make([]byte, size), now.Add(time.Duration(i+1)*time.Millisecond))
						want := error(nil)
						if i == 1 {
							want = io.EOF
						}
						if n != size || err != want {
							t.Fatal("terminal read", n, err)
						}
					}
					if server.streams.window != 6144 {
						t.Fatal("terminal reads did not tune connection")
					}
					after := server.streams.windowSample
					if n, err := server.ReadStreamAt(id, make([]byte, 1), now.Add(3*time.Millisecond)); n != 0 || err != io.EOF {
						t.Fatal(n, err)
					}
					if server.streams.windowSample != after {
						t.Fatal("EOF counted as consumption")
					}
				case "reset", "reset-gap":
					if mode == "reset-gap" {
						// Commit bytes to sending credit, then cancel before emitting them.
						remaining := 2047
						for remaining > 0 {
							n, err := client.WriteStream(id, make([]byte, remaining), false)
							if err != nil || n == 0 {
								t.Fatal(n, err)
							}
							remaining -= n
						}
					}
					if err := client.CancelWrite(id, 17); err != nil {
						t.Fatal(err)
					}
					drivePair(t, client, server, false, false)
				default:
					if err := server.CancelRead(id, 17); err != nil {
						t.Fatal(err)
					}
					drivePair(t, client, server, false, false)
				}
				if state.window != 2048 {
					t.Fatal("terminal/discarded stream grew storage", state.window)
				}
				if server.streams.receive.Consumed() != 2048 {
					t.Fatal("terminal bytes not released", server.streams.receive.Consumed())
				}
				if mode != "read-fin" && (server.streams.window != 3072 || server.streams.windowSample != sample) {
					t.Fatal("discarded data counted as application consumption")
				}
				// Final size is known in every branch, so later accounting must not
				// advertise unnecessary stream credit or grow its receive capacity.
				before := server.streams.windowSample
				if err := server.updateReceiveCredit(id, state); err != nil {
					t.Fatal(err)
				}
				if server.streams.windowSample != before {
					t.Fatal("credit update changed tuning sample")
				}
				for _, item := range server.spaces[applicationSpace].queue {
					if frame, ok := item.frame.(wire.MaxStreamDataFrame); ok && frame.StreamID == id {
						t.Fatal("terminal stream credit advertised")
					}
				}
			})
		}
	}
}
