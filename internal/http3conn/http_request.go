package http3conn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// ReadHTTPRequest adapts a new request MessageReader to a server http.Request.
// Returned ControlData retains extended CONNECT's protocol without inventing a
// regular HTTP header. Negotiation remains in the reader's MessageConfig. Remote
// address, TLS state, handler lifetime and connection policy belong to the server.
// Cleanup ownership and trailer access follow ReadHTTPResponse. Unknown request
// lengths use -1; oversized lengths remain in Header and are validated by the
// message layer without conversion overflow. The caller must close Body.
func ReadHTTPRequest(ctx context.Context, reader *MessageReader, config BodyConfig) (request *http.Request, control ControlData, err error) {
	if ctx == nil || reader == nil || config.Close == nil || reader.message.config.Kind != RequestFields || reader.message.body != nil || reader.err != nil {
		return nil, ControlData{}, fmt.Errorf("http3: invalid HTTP request adapter configuration")
	}
	transferred := false
	defer func() {
		if !transferred {
			config.Close(false)
		}
	}()
	for {
		event, e := reader.Next(ctx)
		if e != nil {
			return nil, ControlData{}, e
		}
		if event.Header.Type != h3.Headers {
			if _, e := io.Copy(io.Discard, reader); e != nil {
				return nil, ControlData{}, e
			}
			continue
		}
		if event.Section != InitialSection {
			return nil, ControlData{}, &h3.Error{Code: h3.FrameUnexpected, Reason: "request starts without initial headers"}
		}
		control = event.Control
		target := control.Path
		var parsed *url.URL
		if control.Method == "CONNECT" && control.Protocol == "" {
			target = control.Authority
			parsed = &url.URL{Host: control.Authority}
		} else {
			if strings.Contains(target, "#") {
				return nil, ControlData{}, &MessageError{"fragment in request target"}
			}
			parsed, e = url.ParseRequestURI(target)
			if e != nil {
				return nil, ControlData{}, &MessageError{"invalid request target"}
			}
			// The transport pseudo-fields identify scheme and authority; the target
			// must not override them through absolute-form or userinfo parsing.
			if parsed.IsAbs() || parsed.Host != "" || parsed.User != nil {
				return nil, ControlData{}, &MessageError{"invalid request target form"}
			}
			// Server requests retain origin-form URLs, as in net/http.
			// Authority belongs in Request.Host; pseudo-fields remain in control.
		}
		if control.Authority != "" {
			authority, e := url.Parse("//" + control.Authority)
			if e != nil || authority.Host != control.Authority || authority.User != nil || authority.Path != "" || authority.RawQuery != "" || authority.ForceQuery || authority.Fragment != "" {
				return nil, ControlData{}, &MessageError{"invalid request authority"}
			}
		}
		headers, e := responseHeaderMap(event.Fields, false)
		if e != nil {
			return nil, ControlData{}, e
		}
		if _, present := headers["Early-Data"]; present {
			headers.Set("Early-Data", "1")
		}
		headers.Del("Host") // net/http stores request authority in Request.Host.
		if cookies := headers.Values("Cookie"); len(cookies) > 1 {
			headers.Set("Cookie", strings.Join(cookies, "; "))
		}
		trailers, e := announcedTrailers(headers.Values("Trailer"))
		if e != nil {
			return nil, ControlData{}, e
		}
		headers.Del("Trailer")
		length := int64(-1)
		if text := headers.Get("Content-Length"); text != "" {
			if n, e := strconv.ParseInt(text, 10, 64); e == nil {
				length = n
			}
		}
		if control.Method == "CONNECT" {
			length = -1
		}
		request = (&http.Request{Method: control.Method, URL: parsed, Host: control.Authority, RequestURI: target, Proto: "HTTP/3.0", ProtoMajor: 3, ProtoMinor: 0, Header: headers, Trailer: trailers, ContentLength: length}).WithContext(ctx)
		if control.Protocol != "" {
			request, _ = WithExtendedConnectProtocol(request, control.Protocol)
		}
		bodyConfig := config
		bodyConfig.Trailers = func(fields []qpack.DecodedField) error {
			if config.Trailers != nil {
				if e := config.Trailers(fields); e != nil {
					return e
				}
			}
			for _, field := range fields {
				request.Trailer.Add(field.Name, field.Value)
			}
			return nil
		}
		request.Body, e = NewMessageBody(ctx, reader, bodyConfig)
		if e != nil {
			return nil, ControlData{}, e
		}
		transferred = true
		return request, control, nil
	}
}
