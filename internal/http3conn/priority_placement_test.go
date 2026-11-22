package http3conn

import (
	"errors"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"testing"
)

func TestPriorityUpdateMessagePlacement(t *testing.T) {
	for _, kind := range []FieldContext{RequestFields, ResponseFields} {
		for _, typ := range []uint64{h3.PriorityUpdateRequest, h3.PriorityUpdatePush} {
			m, setupErr := NewMessage(MessageConfig{Kind: kind, RequestMethod: "GET"})
			if setupErr != nil {
				t.Fatal(setupErr)
			}
			err := m.OtherFrame(typ)
			var protocol *h3.Error
			if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected {
				t.Fatal(err)
			}
		}
	}
}
