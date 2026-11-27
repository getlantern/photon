package recovery

// packetOrder lists packet numbers in the order they were added, with O(1)
// removal by node. A space keeps two: all tracked packets in send order, so
// ACK processing and loss detection visit packets oldest first and stop
// early, and the pure ACKs, whose timestamps are expendable when the ledger
// fills, in eviction order. Each node belongs to one list and is removed
// exactly once by its index.
type packetNode struct {
	prev, next *packetNode
	number     uint64
}

type packetOrder struct {
	front, back *packetNode
	length      int
}

func (o *packetOrder) Len() int           { return o.length }
func (o *packetOrder) Front() *packetNode { return o.front }
func (o *packetOrder) PushBack(number uint64) *packetNode {
	node := &packetNode{prev: o.back, number: number}
	if o.back == nil {
		o.front = node
	} else {
		o.back.next = node
	}
	o.back = node
	o.length++
	return node
}
func (o *packetOrder) Remove(node *packetNode) {
	if node.prev == nil {
		o.front = node.next
	} else {
		node.prev.next = node.next
	}
	if node.next == nil {
		o.back = node.prev
	} else {
		node.next.prev = node.prev
	}
	node.prev, node.next = nil, nil
	o.length--
}
