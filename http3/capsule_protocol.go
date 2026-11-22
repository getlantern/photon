package http3

import (
	"github.com/getlantern/photon/internal/http3conn"
	"net/http"
)

// CapsuleProtocol reports a valid true Capsule-Protocol Structured Field, ignoring
// unknown parameters. False, malformed, repeated and non-Boolean values return
// false. The header alone does not authorize capsules: the extension and a
// successful CONNECT response must establish their use.
func CapsuleProtocol(header http.Header) bool { return http3conn.CapsuleProtocol(header) }
