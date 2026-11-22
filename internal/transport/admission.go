package transport

import (
	"context"
	"crypto/rand"
	"errors"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

// AdmitInitial is a policy-controlled admission step for an unknown
// connection. A router must first dispatch existing destination CIDs, enforce
// admission/rate limits, and associate address with the actual UDP source.
// It returns either a Retry datagram, a started Conn that has consumed data, or
// neither for discarded input. The caller owns any returned Conn even on error
// and must service its closing output before disposal where applicable.
//
// Template supplies Version, TLS and Parameters. Routing IDs are generated or
// recovered here. Authenticated but invalid Retry contents produce a stateless
// INVALID_TOKEN close. Unreadable tokens carry no validation evidence
// (RFC 9000 EID 7861). AddressTokens optionally accepts NEW_TOKEN evidence.
// Invalid recognized NEW_TOKEN values never validate the address.
// VerifySourceAddress may permit admission without token evidence;
// such connections retain the anti-amplification limit until Handshake validation.
// A nil policy requires Retry. 0-RTT admission and Version Negotiation remain separate.
func AdmitInitial(ctx context.Context, template Config, tokens *token.RetryTokens, data []byte, address netip.AddrPort, now time.Time) (*Conn, []byte, error) {
	return AdmitInitialECN(ctx, template, tokens, data, address, now, ECNUnknown)
}

// AdmitInitialECN preserves socket metadata when the admitted Initial
// creates a connection. Stateless Retry and rejection do not count packets.
func AdmitInitialECN(ctx context.Context, template Config, tokens *token.RetryTokens, data []byte, address netip.AddrPort, now time.Time, marking ECN) (_ *Conn, _ []byte, admissionErr error) {
	if marking > ECNCE && marking != ECNUnknown {
		return nil, nil, errors.New("quic: invalid ECN codepoint")
	}

	if template.AdmissionCIDLength == 0 && template.GenerateConnectionID == nil {
		template.AdmissionCIDLength = 8
	}
	if template.AdmissionCIDLength < 0 || template.AdmissionCIDLength > 20 {
		return nil, nil, errors.New("quic: invalid admission CID length")
	}
	if tokens == nil || !template.Version.Supported() || template.TLS == nil {
		return nil, nil, errors.New("quic: invalid admission configuration")
	}
	if !address.IsValid() || address.Port() == 0 || len(data) < 1200 || len(data) > 65527 {
		return nil, nil, nil
	}
	h, err := wire.ParseHeaderWithQUICBit(data, 0, template.Parameters.GreaseQUICBit)
	if err != nil || h.Type != wire.Initial || h.Version != template.Version || len(h.DCID) < 8 {
		return nil, nil, nil
	}
	// Authenticate the Initial packet before responding or allocating TLS state.
	// Initial keys are public; this is structural validation, not peer identity.
	read, _, err := packet.InitialKeys(h.Version, h.DCID)
	if err != nil {
		return nil, nil, nil
	}
	if _, plain, _, err := read.Open(data[:h.PacketLen], h.PNOffset, 0); err != nil || len(plain) == 0 {
		return nil, nil, nil
	}
	newToken := token.IsAddressToken(h.Token)
	addressValidated := newToken && template.AddressTokens != nil && template.AddressTokens.Validate(h.Token, address, h.Version, now) == nil
	var original []byte
	if !addressValidated && len(h.Token) > 0 {
		original, err = tokens.Validate(h.Token, address, h.Version, h.DCID, now)
		if errors.Is(err, token.ErrReadableRetryToken) {
			response, closeErr := invalidTokenClose(h)
			return nil, response, closeErr
		}
	}
	verified := addressValidated || original != nil
	if !verified && (template.VerifySourceAddress == nil || template.VerifySourceAddress(address)) {
		retryCID := make([]byte, 8)
		if _, err := rand.Read(retryCID); err != nil {
			return nil, nil, err
		}
		opaque, err := tokens.Issue(address, h.Version, h.DCID, retryCID, now)
		if err != nil {
			return nil, nil, err
		}
		response, err := packet.BuildRetry(h.Version, h.DCID, h.SCID, retryCID, opaque)
		if err != nil {
			return nil, nil, err
		}
		// The fixed CID and bounded token format fit below one incoming Initial,
		// and therefore within the three-times amplification limit.
		if len(response) > len(data) {
			return nil, nil, ErrAmplification
		}
		return nil, response, nil
	}
	var cancelLifetimeContext context.CancelCauseFunc
	transferred := false
	if hook := template.ConnContext; hook != nil {
		template.ConnContext = nil
		// The connection owner controls shutdown. Preserve admission values without
		// coupling the lifetime to the socket loop's cancellation order.
		base, cancelBase := context.WithCancelCause(context.WithoutCancel(ctx))
		cancelLifetimeContext = cancelBase
		defer func() {
			if !transferred {
				cancelLifetimeContext(admissionErr)
			}
		}()
		selected, hookErr := hook(base, address, verified)
		if selected == nil && hookErr == nil {
			hookErr = errors.New("quic: ConnContext returned nil context")
		}
		if hookErr != nil {
			response, closeErr := admissionClose(h, 0x2)
			return nil, response, errors.Join(hookErr, closeErr)
		}
		lifetime, cancelLifetime := context.WithCancelCause(selected)
		ctx = lifetime
		cancelLifetimeContext = func(err error) {
			cancelLifetime(err)
			cancelBase(err)
		}
	}
	template.AddressVerified = verified
	template.VerifySourceAddress = nil
	if selectConfig := template.ConfigureClient; selectConfig != nil {
		// Never retain the listener callback on a connection, or recursively
		// invoke a callback returned by the selector.
		template.ConfigureClient = nil
		selected, selectErr := selectConfig(template, address)
		if selectErr != nil {
			response, closeErr := admissionClose(h, 0x2)
			return nil, response, errors.Join(selectErr, closeErr)
		}
		selected.ConnContext = nil
		selected.ConfigureClient = nil
		selected.VerifySourceAddress = nil
		selected.AddressVerified = verified
		selected.Version = template.Version
		selected.AddressTokens = template.AddressTokens
		selected.ResetKey = template.ResetKey
		selected.AdmissionCIDLength = template.AdmissionCIDLength
		selected.GenerateConnectionID = template.GenerateConnectionID
		template = selected
	}
	template.Server = true
	if original == nil {
		template.OriginalDCID = h.DCID
		template.RetrySourceCID = nil
	} else {
		template.OriginalDCID = original
		template.RetrySourceCID = h.DCID
	}
	template.PeerCID = h.SCID
	template.LocalCID, err = generateLocalCID(template.AdmissionCIDLength, template.GenerateConnectionID)
	if err != nil {
		return nil, nil, err
	}
	c, err := NewConn(template)
	if err != nil {
		return nil, nil, err
	}
	if cancelLifetimeContext != nil {
		c.lifetimeContext, c.cancelLifetimeContext = ctx, cancelLifetimeContext
		transferred = true
	}
	if err := c.Start(ctx); err != nil {
		c.Close()
		return nil, nil, err
	}
	if verified {
		c.budget.Validate()
	}
	err = c.HandleDatagramECN(data, now, marking)
	return c, nil, err
}

// invalidTokenClose consumes no connection state. Repeated requests produce the
// same packet under the same Initial key, avoiding nonce reuse with different
// plaintext. This non-ack-eliciting Initial does not require 1200-byte padding.
func invalidTokenClose(h wire.Header) ([]byte, error) {
	return admissionClose(h, 0xb)
}

// admissionClose includes only a fixed transport error code, never caller text.
func admissionClose(h wire.Header, code uint64) ([]byte, error) {
	_, write, err := packet.InitialKeys(h.Version, h.DCID)
	if err != nil {
		return nil, err
	}
	payload, err := (wire.ConnectionCloseFrame{ErrorCode: code}).Append(nil)
	if err != nil {
		return nil, err
	}
	header, err := wire.AppendLongHeader(nil, h.Version, wire.Initial, h.SCID, h.DCID, nil, 0, 4, len(payload)+16)
	if err != nil {
		return nil, err
	}
	return write.Seal(header, payload, 0)
}
