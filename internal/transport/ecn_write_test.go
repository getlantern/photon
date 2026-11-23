//go:build linux || darwin

package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
	"golang.org/x/sys/unix"
)

func TestECNWriteFallback(t *testing.T) {
	for _, cause := range []error{unix.EINVAL, unix.ENOPROTOOPT, unix.EOPNOTSUPP} {
		sent, _ := recovery.NewSent(4)
		sent.EnableECN()
		if err := sent.OnSent(recovery.Application, recovery.Packet{ECN: recovery.ECT0, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
		out := &Outbound{ECN: recovery.ECT0, Data: []byte("ciphertext"), ecn: ecnCommit{sent, recovery.Application, 0}}
		calls := 0
		n, err := writeWithECNFallback(out, func(mark recovery.ECNMark) (int, error) {
			calls++
			if calls == 1 {
				if mark != recovery.ECT0 {
					t.Fatal(mark)
				}
				return 0, fmt.Errorf("sendmsg: %w", cause)
			}
			if calls != 2 || mark != recovery.NotECT || string(out.Data) != "ciphertext" {
				t.Fatal("bad fallback", calls, mark)
			}
			return len(out.Data), nil
		})
		if err != nil || n != len(out.Data) || calls != 2 || out.ECN != recovery.NotECT {
			t.Fatal(n, err, calls)
		}
		if sent.ECNSent(recovery.Application) != [2]uint64{} || sent.BytesInFlight != 1200 || sent.ECNState() != recovery.ECNFailed {
			t.Fatal("fallback accounting")
		}
		ev, err := sent.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, time.Now(), 0, 0, true)
		if err != nil || len(ev.Acked) != 1 || ev.Acked[0].ECN != recovery.NotECT || ev.NewlyECT != [2]uint64{} {
			t.Fatal("fallback packet not ACKable", ev, err)
		}
	}
}

func TestECNWriteDoesNotRetryOtherOutcomes(t *testing.T) {
	for _, tc := range []struct {
		n   int
		err error
	}{{1, unix.EINVAL}, {0, unix.EMSGSIZE}, {0, unix.ECONNREFUSED}, {10, nil}} {
		sent, _ := recovery.NewSent(4)
		sent.EnableECN()
		if err := sent.OnSent(recovery.Application, recovery.Packet{ECN: recovery.ECT0}); err != nil {
			t.Fatal(err)
		}
		out := &Outbound{ECN: recovery.ECT0, ecn: ecnCommit{sent, recovery.Application, 0}}
		calls := 0
		n, err := writeWithECNFallback(out, func(recovery.ECNMark) (int, error) { calls++; return tc.n, tc.err })
		if calls != 1 || n != tc.n || !errors.Is(err, tc.err) || sent.ECNSent(recovery.Application) != [2]uint64{1, 0} {
			t.Fatal("unrelated send outcome retried", n, err, calls)
		}
	}
}
