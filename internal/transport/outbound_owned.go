package transport

// retain makes borrowed wire bytes and descriptor metadata independently owned.
// Connection, path and send-item identity pointers deliberately remain shared:
// this result stays on the connection owner, never on a foreign socket owner.
func (out *Outbound) retain() *Outbound {
	owned := *out
	owned.Data = append([]byte(nil), out.Data...)
	if out.migration != nil {
		value := *out.migration
		owned.migration = &value
	}
	if out.mtu != nil {
		value := *out.mtu
		owned.mtu = &value
	}
	if out.large != nil {
		value := *out.large
		owned.large = &value
	}
	if out.datagram != nil {
		value := *out.datagram
		owned.datagram = &value
	}
	if out.baseLarge != nil {
		value := *out.baseLarge
		owned.baseLarge = &value
	}
	return &owned
}
