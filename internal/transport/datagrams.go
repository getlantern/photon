package transport

import (
	"errors"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

var ErrDatagramUnsupported = errors.New("quic: peer does not support datagrams")
var ErrDatagramTooLarge = errors.New("quic: datagram exceeds peer or packet size limit")

// DatagramTooLargeError reports the current admission limit, including peer
// frame credit and the selected path's packet budget. The limit can change.
type DatagramTooLargeError struct {
	MaxDatagramPayloadSize int64
}

func (*DatagramTooLargeError) Error() string { return "DATAGRAM frame too large" }
func (e *DatagramTooLargeError) Is(target error) bool {
	t, ok := target.(*DatagramTooLargeError)
	return ok && t != nil && e.MaxDatagramPayloadSize == t.MaxDatagramPayloadSize
}
func (*DatagramTooLargeError) Unwrap() error { return ErrDatagramTooLarge }

const maxBufferedDatagrams = 64
const maxDatagramBytes = 64 * 1024

type datagramBuffer struct {
	queue [][]byte
	bytes int
}

// SendDatagram copies and queues an unreliable message. ErrWouldBlock means
// usable traffic keys or queue capacity are needed. No delivery is guaranteed.
// The selected path budget reserves the largest connection ID, so CID rotation
// cannot make an already queued message too large. Messages are not fragmented;
// a queued message may be discarded if its path size subsequently decreases.
func (c *Conn) SendDatagram(data []byte) error {
	if err := c.check(); err != nil {
		return err
	}
	// Early messages use only the remembered limit and reserve both long-header
	// CIDs. Servers can use live limits with 1-RTT keys before client Finished;
	// clients use live limits only after completing TLS.
	peerLimit := uint64(0)
	maxPayload := 1200 - 1 - 20 - 4 - 16 - 1
	if c.sendingEarly() {
		peerLimit = c.streams.peerLimits.MaxDatagramFrameSize
		maxPayload = 1200 - (1 + 4 + 1 + 20 + 1 + 20 + 2 + 4 + 16) - 1
	} else {
		if (!c.complete && !c.serverApplicationReady()) || c.peerParams == nil {
			return stream.ErrWouldBlock
		}
		peerLimit = c.peerParams.MaxDatagramFrameSize
		maxPayload = c.basePacketPayloadLimit() - (1 + 20 + 4 + 16 + 1)
		if c.multipath != nil && c.confirmed {
			if id, ok := c.applicationEnqueuePath(); ok {
				maxPayload = c.multipath.paths[id].application.packetPayloadLimit() - (1 + 20 + 4 + 16 + 1)
			}
		}
	}
	if peerLimit == 0 {
		return ErrDatagramUnsupported
	}
	// One type byte; omit the length because each packet contains one frame.
	limit := min(uint64(maxPayload), peerLimit-1)
	if uint64(len(data)) > limit {
		return &DatagramTooLargeError{MaxDatagramPayloadSize: int64(limit)}
	}
	size := len(data) + 1
	if c.pendingDatagrams >= maxBufferedDatagrams || size > c.availableSendBytes() {
		return stream.ErrWouldBlock
	}
	if err := c.enqueue(applicationSpace, wire.DatagramFrame{Data: clone(data), OmitLength: true}, size); err != nil {
		return err
	}
	return nil
}

// ReceiveDatagram transfers ownership of the next received message. An empty
// message returns success; an empty queue returns ErrWouldBlock.
func (c *Conn) ReceiveDatagram() ([]byte, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if len(c.datagrams.queue) == 0 {
		return nil, stream.ErrWouldBlock
	}
	data := c.datagrams.queue[0]
	c.datagrams.queue[0] = nil
	c.datagrams.queue = c.datagrams.queue[1:]
	c.datagrams.bytes -= len(data)
	return data, nil
}

func (c *Conn) receiveDatagram(data []byte) {
	if len(c.datagrams.queue) >= maxBufferedDatagrams || len(data) > maxDatagramBytes-c.datagrams.bytes {
		return
	}
	c.datagrams.queue = append(c.datagrams.queue, clone(data))
	c.datagrams.bytes += len(data)
}

// datagramFitsPath includes the actual short header, four-byte packet number,
// authentication tag and frame encoding. No packet number or ciphertext is
// committed when this check rejects an unsent message.
func datagramFitsPath(f wire.DatagramFrame, limit, cidLength int) bool {
	overhead := 1 + cidLength + 4 + 16 + 1
	if !f.OmitLength {
		overhead += wire.VarintLen(uint64(len(f.Data)))
	}
	return len(f.Data) <= limit-overhead
}
