package wire

import "encoding/binary"

// VersionInformation is RFC 9368's authenticated transport parameter. For a
// client, Available lists compatible first-flight versions in preference order.
// For a server, it lists fully deployed versions and can be empty.
type VersionInformation struct {
	Chosen    Version
	Available []Version
}

func (v *VersionInformation) validate(server bool) error {
	if v.Chosen == 0 {
		return parameterError(0x11, "zero chosen version")
	}
	found := false
	for _, available := range v.Available {
		if available == 0 {
			return parameterError(0x11, "zero available version")
		}
		if available == v.Chosen {
			found = true
		}
	}
	if !server && !found {
		return parameterError(0x11, "client chosen version missing from available versions")
	}
	return nil
}
func parseVersionInformation(value []byte, server bool) (*VersionInformation, error) {
	if len(value) < 4 || len(value)%4 != 0 {
		return nil, parameterError(0x11, "invalid version information length")
	}
	v := &VersionInformation{Chosen: Version(binary.BigEndian.Uint32(value))}
	for i := 4; i < len(value); i += 4 {
		v.Available = append(v.Available, Version(binary.BigEndian.Uint32(value[i:])))
	}
	if err := v.validate(server); err != nil {
		return nil, err
	}
	return v, nil
}
func (v *VersionInformation) marshal() []byte {
	value := binary.BigEndian.AppendUint32(nil, uint32(v.Chosen))
	for _, available := range v.Available {
		value = binary.BigEndian.AppendUint32(value, uint32(available))
	}
	return value
}
