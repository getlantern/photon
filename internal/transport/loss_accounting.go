package transport

import "github.com/getlantern/photon/internal/recovery"

// lossAccounting belongs to the connection owner, not a path. Key discard,
// Retry and path abandonment are not loss declarations. Recovery history can
// forget late-ACK evidence, so corrections cover only its retained horizon.
type lossAccounting struct {
	declaredPackets, declaredBytes   uint64
	recoveredPackets, recoveredBytes uint64
}

func (s *lossAccounting) lost(packets []recovery.Packet) {
	s.declaredPackets += uint64(len(packets))
	for _, p := range packets {
		s.declaredBytes += uint64(p.Bytes)
	}
}

func (s *lossAccounting) recovered(events recovery.Events) {
	s.recoveredPackets += events.RecoveredPackets
	s.recoveredBytes += events.RecoveredBytes
}
