package http3

import (
	"crypto/sha256"
	"fmt"
	"slices"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

// Reserve room for the five settings managed by the implementation within the
// endpoint's 64-entry SETTINGS budget, independent of which extensions are on.
const maxAdditionalSettings = 59

func copyAdditionalSettings(source map[uint64]uint64) (map[uint64]uint64, error) {
	if len(source) > maxAdditionalSettings {
		return nil, fmt.Errorf("http3: too many additional settings")
	}
	if source == nil {
		return nil, nil
	}
	result := make(map[uint64]uint64, len(source))
	for id, value := range source {
		if id > wire.MaxVarint || value > wire.MaxVarint {
			return nil, fmt.Errorf("http3: additional setting out of range")
		}
		if (id >= 1 && id <= 8) || id == h3.H3Datagram {
			return nil, fmt.Errorf("http3: setting %x is reserved or managed by the implementation", id)
		}
		result[id] = value
	}
	return result, nil
}

func appendAdditionalSettings(settings []h3.Setting, extra map[uint64]uint64) []h3.Setting {
	ids := make([]uint64, 0, len(extra))
	for id := range extra {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		settings = append(settings, h3.Setting{ID: id, Value: extra[id]})
	}
	return settings
}

// Unknown extension semantics cannot determine whether a changed value is safe
// for 0-RTT. Bind the exact configured map; changes preserve ordinary resumption.
// Empty maps retain the pre-extension ticket scope.
func additionalSettingsScope(extra map[uint64]uint64) string {
	if len(extra) == 0 {
		return ""
	}
	data, _ := h3.AppendSettings(nil, appendAdditionalSettings(nil, extra)) // validated by caller
	return fmt.Sprintf("|additional=%x", sha256.Sum256(data))
}

func (t *Transport) clientOptions() (ClientConfig, error) {
	if t.EnableDatagrams && t.QUICConfig != nil && !t.QUICConfig.EnableDatagrams {
		return ClientConfig{}, fmt.Errorf("HTTP Datagrams enabled, but QUIC Datagrams disabled")
	}
	base := ClientConfig{}
	if t.ClientConfig != nil {
		base = *t.ClientConfig
	}
	base.EnableDatagrams = base.EnableDatagrams || t.EnableDatagrams
	if t.MaxResponseHeaderBytes > 0 {
		base.responseHeaderBytes = t.MaxResponseHeaderBytes
	} else if base.MaxHeaderBytes == 0 {
		base.responseHeaderBytes = 10 << 20
	}
	cfg, err := clientOptions(&base)
	if err != nil {
		return cfg, err
	}
	extra, err := copyAdditionalSettings(t.AdditionalSettings)
	if err != nil {
		return cfg, err
	}
	if cfg.AdditionalSettings == nil {
		cfg.AdditionalSettings = extra
		return cfg, nil
	}
	for id, value := range extra {
		if _, exists := cfg.AdditionalSettings[id]; exists {
			return cfg, fmt.Errorf("http3: duplicate additional setting %x in transport and client configuration", id)
		}
		cfg.AdditionalSettings[id] = value
	}
	if len(cfg.AdditionalSettings) > maxAdditionalSettings {
		return cfg, fmt.Errorf("http3: too many additional settings")
	}
	return cfg, nil
}
