package recovery

// ACK-only timestamps are expendable when the sent-packet ledger fills. Keep
// their eviction order without boxing packet numbers in interface-valued nodes.
// Each node belongs to one packet space and is removed exactly once by its index.
type ackOnlyNode struct {
	prev, next *ackOnlyNode
	number     uint64
}

type ackOnlyOrder struct {
	front, back *ackOnlyNode
	length      int
}

func (o *ackOnlyOrder) Len() int            { return o.length }
func (o *ackOnlyOrder) Front() *ackOnlyNode { return o.front }
func (o *ackOnlyOrder) PushBack(number uint64) *ackOnlyNode {
	node := &ackOnlyNode{prev: o.back, number: number}
	if o.back == nil {
		o.front = node
	} else {
		o.back.next = node
	}
	o.back = node
	o.length++
	return node
}
func (o *ackOnlyOrder) Remove(node *ackOnlyNode) {
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
