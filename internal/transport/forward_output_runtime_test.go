package transport

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForwardOutputRuntimeProbeAndSwitch(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(v.String(), func(t *testing.T) {
			source, destination := forwardingRuntime(t), forwardingRuntime(t)
			defer source.Close()
			defer destination.Close()
			c, peer := testPair(t, v, streamParameters())
			drivePair(t, c, peer, false, false)
			c.config.EnableMigration = true
			c.peerParams.DisableActiveMigration = false
			sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer sink.Close()
			old := destination.Addr().(*net.UDPAddr).AddrPort()
			next := source.Addr().(*net.UDPAddr).AddrPort()
			remote := sink.LocalAddr().(*net.UDPAddr).AddrPort()
			if err := peer.activateBasePath(remote, old); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := destination.Do(ctx, func(e *ServerEndpoint) error {
				if err := e.AddClient(c, remote); err != nil {
					return err
				}
				return e.networks[c].activate(time.Now())
			}); err != nil {
				t.Fatal(err)
			}
			h, err := destination.handoffTarget(ctx)
			if err != nil {
				t.Fatal(err)
			}
			route, err := source.registerHandoffRoute(ctx, h, nil)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := source.foreignWriteTarget(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := destination.bindHandoffConnection(ctx, route, c); err != nil {
				t.Fatal(err)
			}
			var candidate *migrationCandidate
			if err := destination.Do(ctx, func(e *ServerEndpoint) error {
				if err := e.bindForeignWriter(c, writer, route); err != nil {
					return err
				}
				candidate, err = c.prepareLocalPathProbe(0, next, time.Now())
				return err
			}); err != nil {
				t.Fatal(err)
			}
			deadline, _ := ctx.Deadline()
			sink.SetReadDeadline(deadline)
			var storage [2048]byte
			var challenge wire.PathChallengeFrame
			for {
				n, from, err := sink.ReadFromUDPAddrPort(storage[:])
				if err != nil {
					t.Fatal("foreign probe missing", err)
				}
				if from != next {
					continue
				}
				decoded, err := peer.authenticateMultipathPacket(storage[:n], time.Now())
				if err != nil || decoded == nil {
					t.Fatal("invalid foreign probe", err)
				}
				found := false
				for _, frame := range decoded.packet.frames {
					switch f := frame.(type) {
					case wire.PathChallengeFrame:
						challenge = f
						found = true
					default:
						if frame.Type() != 0 {
							t.Fatalf("staged probe carried non-probing frame %x", frame.Type())
						}
					}
				}
				if found {
					break
				}
			}
			response, _ := (wire.PathResponseFrame{Data: challenge.Data}).Append(nil)
			proof := sealPathPayload(t, peer.writeKeys(applicationSpace), peer.peerCID, 0, peer.spaces[applicationSpace].nextPN+20, response)
			if _, err := sink.WriteToUDPAddrPort(proof, next); err != nil {
				t.Fatal(err)
			}
			if err := destination.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
				if c.closed {
					return false, ErrEndpointClosed
				}
				return candidate.validation.state == PathValidated && c.pendingWrite == nil, nil
			}); err != nil {
				t.Fatal("forwarded proof did not validate", err)
			}
			payload := []byte("stream through foreign socket")
			if err := destination.Do(ctx, func(e *ServerEndpoint) error {
				if e.locals[next] {
					return fmt.Errorf("foreign socket became listener-local")
				}
				if candidate.committed || c.multipath.paths[0].application.network.local != old {
					return fmt.Errorf("probe switched automatically")
				}
				if err := c.commitLocalMigration(0, candidate); err != nil {
					return err
				}
				id, err := c.OpenStream(false)
				if err != nil {
					return err
				}
				_, err = c.WriteStream(id, payload, true)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		streamRead:
			for {
				n, from, err := sink.ReadFromUDPAddrPort(storage[:])
				if err != nil {
					t.Fatal("foreign stream missing", err)
				}
				if from != next {
					continue
				}
				decoded, err := peer.authenticateMultipathPacket(storage[:n], time.Now())
				if err != nil || decoded == nil {
					t.Fatal(err)
				}
				for _, frame := range decoded.packet.frames {
					if stream, ok := frame.(wire.StreamFrame); ok && bytes.Equal(stream.Data, payload) {
						break streamRead
					}
				}
			}
			if err := destination.Do(ctx, func(*ServerEndpoint) error { return c.CloseWithError(9, "foreign close", time.Now()) }); err != nil {
				t.Fatal(err)
			}
			for {
				n, from, err := sink.ReadFromUDPAddrPort(storage[:])
				if err != nil {
					t.Fatal("foreign close missing", err)
				}
				if from != next {
					continue
				}
				decoded, err := peer.authenticateMultipathPacket(storage[:n], time.Now())
				if err != nil || decoded == nil {
					t.Fatal(err)
				}
				for _, frame := range decoded.packet.frames {
					if closeFrame, ok := frame.(wire.ConnectionCloseFrame); ok && closeFrame.ErrorCode == 9 && closeFrame.Application {
						return
					}
				}
			}
		})
	}
}
