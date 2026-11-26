package recovery

import "time"

// Delivery-rate estimation, as in draft-cheng-iccrg-delivery-rate-estimation:
// each in-flight packet records, when sent, how much the path had delivered
// and when, so that its acknowledgment can measure the rate at which data was
// delivered over the interval since. Model-based congestion controllers such
// as BBR are built on these samples.

// DeliveryState is what an in-flight packet records about the path when sent.
type DeliveryState struct {
	// Delivered is the bytes the path had delivered, and DeliveredAt when
	// the last of them was acknowledged, or, for a flight started from idle,
	// when its first packet was sent.
	Delivered   uint64
	DeliveredAt time.Time
	// FirstSentAt is when the first packet of the current flight was sent.
	FirstSentAt time.Time
	// AppLimited is true if the sender was application limited, so a sample
	// from this packet may understate what the path can carry.
	AppLimited bool
	// InFlight is the bytes in flight once the packet was sent, itself
	// included.
	InFlight uint64
	// Lost is the path's lost in-flight bytes when the packet was sent.
	Lost uint64
	// CE is the path's ECN-CE-marked bytes when the packet was sent.
	CE uint64
}

// RateSample is one acknowledgment's measurement of the delivery rate.
type RateSample struct {
	// Valid is false when the ACK newly acknowledged no in-flight packet, or
	// its interval was shorter than the minimum RTT and so not trustworthy.
	Valid bool
	// Delivered is the bytes delivered over Interval, and Rate that in bytes
	// per second.
	Delivered uint64
	Interval  time.Duration
	Rate      uint64
	// AppLimited is true if the sample's packet was sent while the sender
	// was application limited: the rate is then a lower bound.
	AppLimited bool
	// RTT is the newest acknowledged packet's round trip, and InFlight the
	// bytes in flight when it was sent.
	RTT      time.Duration
	InFlight uint64
	// Lost is the in-flight bytes declared lost since the newest
	// acknowledged packet was sent, this ACK's losses included.
	Lost uint64
	// CE is the bytes reported ECN-CE-marked since the newest acknowledged
	// packet was sent, and NewCE those this ACK reported. An ACK reports
	// how many packets were marked, not which, so marked bytes are the count
	// times the average size of the packets it newly acknowledged.
	CE, NewCE uint64
	// NewlyAcked is the in-flight bytes this ACK newly acknowledged.
	NewlyAcked uint64
	// PriorDelivered is the path's delivered bytes when the newest
	// acknowledged packet was sent, which marks packet-timed round trips.
	PriorDelivered uint64
	// TotalDelivered is the path's delivered bytes after this ACK.
	TotalDelivered uint64
}

// deliveryRate is the path's delivery state.
type deliveryRate struct {
	delivered       uint64
	lost            uint64
	ce              uint64
	deliveredAt     time.Time
	firstSentAt     time.Time
	appLimitedUntil uint64 // delivered count that ends the app-limited phase; 0 if none
}

// stamp records the path's delivery state on an in-flight packet as it is
// sent, with flight the bytes already in flight before it.
func (d *deliveryRate) stamp(p *Packet, flight uint64) {
	if flight == 0 {
		d.firstSentAt, d.deliveredAt = p.SentAt, p.SentAt
	}
	p.Delivery = DeliveryState{
		Delivered:   d.delivered,
		DeliveredAt: d.deliveredAt,
		FirstSentAt: d.firstSentAt,
		AppLimited:  d.appLimitedUntil != 0,
		InFlight:    flight + uint64(p.Bytes),
		Lost:        d.lost,
		CE:          d.ce,
	}
}

// rateSampler builds one ACK's sample from its newly acknowledged packets.
type rateSampler struct {
	newest      Packet
	have        bool
	sendElapsed time.Duration
	newlyAcked  uint64
	newCE       uint64
}

// marked counts packets this ACK newly reported ECN-CE-marked, at the
// average size of the in-flight packets it newly acknowledged (bytes in
// count packets), or minimum-size packets if it acknowledged none.
func (d *deliveryRate) marked(s *rateSampler, packets, bytes, count uint64) {
	size := uint64(1200)
	if count > 0 {
		size = bytes / count
	}
	s.newCE += packets * size
	d.ce += packets * size
}

// acked counts an in-flight packet as delivered and keeps the most recently
// sent one as the sample's packet. A packet already declared lost and then
// acknowledged late never reaches it, since loss detection removed it from
// the ledger, so its data counts once, with its retransmission, and its loss
// already reached the controller. (TCP's version of the draft does sample
// such a packet, since there it is the same buffer as its retransmission.)
func (d *deliveryRate) acked(s *rateSampler, p Packet, now time.Time) {
	if !p.InFlight {
		return
	}
	d.delivered += uint64(p.Bytes)
	d.deliveredAt = now
	s.newlyAcked += uint64(p.Bytes)
	if !s.have || p.SentAt.After(s.newest.SentAt) || p.SentAt.Equal(s.newest.SentAt) && p.Number > s.newest.Number {
		s.newest, s.have = p, true
		s.sendElapsed = p.SentAt.Sub(p.Delivery.FirstSentAt)
		d.firstSentAt = p.SentAt
	}
}

// sample finishes one ACK's sample. An interval shorter than minRTT is
// discarded: it can only come from ACK compression or reordering, and would
// overstate the rate. Without an RTT sample (sampled false) there is nothing
// to check it against, so no sample is valid.
func (d *deliveryRate) sample(s *rateSampler, now time.Time, minRTT time.Duration, sampled bool) RateSample {
	if d.appLimitedUntil != 0 && d.delivered > d.appLimitedUntil {
		d.appLimitedUntil = 0
	}
	rs := RateSample{TotalDelivered: d.delivered, NewlyAcked: s.newlyAcked, NewCE: s.newCE}
	if !s.have {
		return rs
	}
	p := s.newest.Delivery
	rs.PriorDelivered = p.Delivered
	rs.Lost = d.lost - p.Lost
	rs.CE = d.ce - p.CE
	rs.Delivered = d.delivered - p.Delivered
	rs.Interval = max(s.sendElapsed, d.deliveredAt.Sub(p.DeliveredAt))
	rs.AppLimited = p.AppLimited
	rs.RTT = now.Sub(s.newest.SentAt)
	rs.InFlight = p.InFlight
	if rs.Interval <= 0 || !sampled || rs.Interval < minRTT {
		return rs
	}
	rs.Valid = true
	rs.Rate = uint64(float64(rs.Delivered) * float64(time.Second) / float64(rs.Interval))
	return rs
}

// CEBytes is the path's bytes reported ECN-CE-marked so far.
func (r *Sent) CEBytes() uint64 { return r.rate.ce }

// LostBytes is the path's in-flight bytes declared lost so far, MTU probes
// aside.
func (r *Sent) LostBytes() uint64 { return r.rate.lost }

// SmoothedRTT is the path's smoothed RTT, or zero before any RTT sample.
func (r *Sent) SmoothedRTT() time.Duration {
	if !r.RTT.Sampled {
		return 0
	}
	return r.RTT.Smoothed
}

// DeliveredBytes is the path's in-flight bytes delivered so far.
func (r *Sent) DeliveredBytes() uint64 { return r.rate.delivered }

// AppLimited reports whether the path is in an application-limited phase:
// packets sent now are marked so their samples are known to understate it.
func (r *Sent) AppLimited() bool { return r.rate.appLimitedUntil != 0 }

// MarkAppLimited records that the sender has nothing more to send although
// the window has room. Samples from packets sent until the data now in flight
// is delivered are marked application limited.
func (r *Sent) MarkAppLimited() {
	r.rate.appLimitedUntil = max(r.rate.delivered+r.BytesInFlight, 1)
}
