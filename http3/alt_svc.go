package http3

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// ErrNoAltSvcPort means no listening HTTP/3 endpoint can be advertised.
var ErrNoAltSvcPort = errors.New("http3: no port can be announced; specify Server.Port or Server.Addr")

// SetQUICHeaders appends an Alt-Svc value for the server's active listeners.
// Port overrides listener ports; Addr is used only when no listener has a port.
// Configure those fields before serving. The caller owns and synchronizes hdr.
// Before serving or after all accept loops stop it returns ErrNoAltSvcPort.
func (s *Server) SetQUICHeaders(hdr http.Header) error {
	s.mu.Lock()
	value := s.altSvcHeader
	s.mu.Unlock()
	if value == "" {
		return ErrNoAltSvcPort
	}
	hdr.Add("Alt-Svc", value)
	return nil
}

func altSvcPort(address string) int {
	_, text, err := net.SplitHostPort(address)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(text)
	if err != nil || port < 1 || port > 65535 {
		return 0
	}
	return port
}

// Called with s.mu held; never invokes application listener methods.
func (s *Server) refreshAltSvcLocked() {
	entries := make([]*serverListener, 0, len(s.listeners))
	for entry := range s.listeners {
		if entry.advertise {
			entries = append(entries, entry)
		}
	}
	s.altSvcHeader = ""
	if len(entries) == 0 {
		return
	}
	ports := make([]int, 0, len(entries))
	if s.Port != 0 {
		if s.Port < 1 || s.Port > 65535 {
			return
		}
		ports = append(ports, s.Port)
	} else {
		sort.Slice(entries, func(i, j int) bool { return entries[i].order < entries[j].order })
		for _, entry := range entries {
			if entry.port != 0 {
				ports = append(ports, entry.port)
			}
		}
		if len(ports) == 0 {
			if port := altSvcPort(s.Addr); port != 0 {
				ports = append(ports, port)
			}
		}
	}
	values := make([]string, 0, len(ports))
	for _, port := range ports {
		values = append(values, fmt.Sprintf(`h3=":%d"; ma=2592000`, port))
	}
	s.altSvcHeader = strings.Join(values, ",")
}
