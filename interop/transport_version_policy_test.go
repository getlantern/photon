package interop

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func TestTransportVersionNegotiationPolicyReference(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, disabled := range []bool{false, true} {
			for _, reference := range []bool{false, true} {
				t.Run(fmt.Sprintf("%v/disabled=%v/reference=%v", version, disabled, reference), func(t *testing.T) {
					_, st := tlsConfigs(t)
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					if reference {
						tr := &upstream.Transport{Conn: socket, DisableVersionNegotiationPackets: disabled}
						defer tr.Close()
						if _, err := tr.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}); err != nil {
							t.Fatal(err)
						}
					} else {
						tr := &quic.Transport{Conn: socket, DisableVersionNegotiationPackets: disabled}
						defer tr.Close()
						if _, err := tr.Listen(st, &quic.Config{Versions: []quic.Version{version}}); err != nil {
							t.Fatal(err)
						}
					}
					probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer probe.Close()
					dcid, scid := []byte("unknown0"), []byte("probe000")
					data, err := wire.AppendVersionNegotiation(nil, 0xc0, dcid, scid, []wire.Version{version})
					if err != nil {
						t.Fatal(err)
					}
					binary.BigEndian.PutUint32(data[1:5], 0x12345678)
					data = append(data, make([]byte, 1200-len(data))...)
					if _, err := probe.WriteToUDP(data, socket.LocalAddr().(*net.UDPAddr)); err != nil {
						t.Fatal(err)
					}
					timeout := time.Second
					if disabled {
						timeout = 50 * time.Millisecond
					}
					probe.SetReadDeadline(time.Now().Add(timeout))
					var response [1500]byte
					n, _, err := probe.ReadFromUDP(response[:])
					if disabled {
						timed, ok := err.(net.Error)
						if !ok || !timed.Timeout() {
							t.Fatal("unexpected response", n, err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					h, err := wire.ParseHeader(response[:n], 0)
					if err != nil || h.Version != 0 || !bytes.Equal(h.DCID, scid) || !bytes.Equal(h.SCID, dcid) {
						t.Fatal(h, err)
					}
					found := false
					for _, offered := range h.Versions {
						found = found || offered == version
					}
					if !found {
						t.Fatal("configured version absent", h.Versions)
					}
				})
			}
		}
	}
}
