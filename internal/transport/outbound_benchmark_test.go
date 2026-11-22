package transport

import (
	"testing"
)

var outboundEnvelopeSink *Outbound

// This isolates envelope allocation for packets that need no MTU error metadata.
// End-to-end comparisons remain necessary for packet-processing throughput.
func BenchmarkOutboundEnvelope(b *testing.B) {
	payload := make([]byte, 1200)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outboundEnvelopeSink = &Outbound{Data: payload}
	}
}

var outboundMetadataSink *outboundBaseLarge

func BenchmarkBaseWriteMetadata(b *testing.B) {
	for _, borrowed := range []bool{false, true} {
		name := "owned"
		if borrowed {
			name = "reused"
		}
		b.Run(name, func(b *testing.B) {
			c := &Conn{confirmed: true, lastBaseApplication: true, lastBaseDatagram: true}
			c.spaces[applicationSpace].nextPN = 1
			var storage *outboundBaseLarge
			if borrowed {
				storage = new(outboundBaseLarge)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				outboundMetadataSink = c.outboundBaseLarge(1280, storage)
			}
		})
	}
}
