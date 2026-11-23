package transport

import "github.com/getlantern/photon/internal/wire"

// Bound retained idle payload capacity to 32 KiB per active connection. This cache is
// owned by the serialized engine; buffers never cross connection boundaries.
const maxCachedSendBuffers = 32
const maxCachedSendBufferBytes = 32 * 1024
const maxCachedSendBufferSize = 2048

func (c *Conn) takeSendBuffer(size int) []byte {
	if size == 0 || size > maxCachedSendBufferSize {
		return nil
	}
	for i := len(c.sendBuffers) - 1; i >= 0; i-- {
		buffer := c.sendBuffers[i]
		if cap(buffer) < size {
			continue
		}
		last := len(c.sendBuffers) - 1
		c.sendBuffers[i] = c.sendBuffers[last]
		c.sendBuffers[last] = nil
		c.sendBuffers = c.sendBuffers[:last]
		c.cachedSendBufferBytes -= cap(buffer)
		return buffer[:0]
	}
	return nil
}

func (c *Conn) putSendBuffer(buffer []byte) {
	if cap(buffer) == 0 || cap(buffer) > maxCachedSendBufferSize || len(c.sendBuffers) == maxCachedSendBuffers || cap(buffer) > maxCachedSendBufferBytes-c.cachedSendBufferBytes {
		return
	}
	c.sendBuffers = append(c.sendBuffers, buffer[:0])
	c.cachedSendBufferBytes += cap(buffer)
}

// Called only after successful logical completion. Split groups retain their
// payload until ordinary garbage collection; a partial fragment ACK is never
// sufficient to recycle shared data. Stale packet/queue references are guarded
// by acked before they can inspect the frame again.
func (c *Conn) recycleStreamItem(item *sendItem) {
	if item.delivery != nil && item.delivery.ledger != nil {
		return
	}
	frame, ok := item.frame.(wire.StreamFrame)
	if !ok {
		return
	}
	c.putSendBuffer(frame.Data)
	item.frame = nil
}
