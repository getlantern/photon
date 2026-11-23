package http3conn

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/getlantern/photon/internal/qpack"
)

// WriteHTTPRequest owns one upload on a fresh request MessageWriter. It closes
// request.Body exactly once, including on invalid request metadata or I/O failure.
// The owner must not write concurrently and must reset failed uploads and apply
// connection error scope. interrupt must unblock stream writes without waiting
// for this call; cancellation also closes Body to unblock its Read. The body must
// implement net/http's concurrent Read/Close contract. Bodies implementing
// CloseWithError(error) receive upload failures, allowing pipe writers to retain
// the cause. Successful uploads use Close. Request trailers may update
// values while Body is read, but may not introduce new names after headers.
// Run upload and response reception concurrently at the endpoint so early server
// responses cannot deadlock on an unfinished upload. CONNECT tunnel ownership and
// Expect/continue scheduling require separate endpoint policy.
func WriteHTTPRequest(ctx context.Context, request *http.Request, writer *MessageWriter, interrupt func()) (err error) {
	if ctx == nil || request == nil || writer == nil || interrupt == nil || writer.message.config.Kind != RequestFields || writer.message.body != nil || writer.closed || writer.err != nil {
		return fmt.Errorf("http3: invalid request upload configuration")
	}
	var closeOnce sync.Once
	var closeErr error
	closeBody := func(cause error) {
		closeOnce.Do(func() {
			if request.Body != nil {
				if closer, ok := request.Body.(interface{ CloseWithError(error) error }); ok && cause != nil {
					closeErr = closer.CloseWithError(cause)
				} else {
					closeErr = request.Body.Close()
				}
			}
		})
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(interrupted); interrupt(); closeBody(context.Cause(ctx)) })
	defer func() {
		if !stop() {
			<-interrupted
		}
		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		closeBody(err)
		if err == nil {
			err = closeErr
		}
	}()
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	headers, err := HTTPRequestHeaders(request)
	if err != nil {
		return err
	}
	declared := make(map[string]bool, len(request.Trailer))
	var names []qpack.DecodedField
	for name := range request.Trailer {
		name = strings.ToLower(name)
		if !declared[name] {
			declared[name] = true
			names = append(names, qpack.DecodedField{Field: qpack.Field{Name: name}})
		}
	}
	if len(names) > 0 {
		if err := ValidateOutgoingTrailers(names, writer.permitsTrailer); err != nil {
			return err
		}
	}
	if err := writer.Headers(headers); err != nil {
		return err
	}
	if request.Body != nil && request.Body != http.NoBody {
		buffer := make([]byte, 32*1024)
		empty := 0
		for {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			n, readErr := request.Body.Read(buffer)
			if n < 0 || n > len(buffer) {
				return fmt.Errorf("http3: invalid request body read count")
			}
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			if n > 0 {
				empty = 0
				if _, err := writer.Write(buffer[:n]); err != nil {
					return err
				}
			} else if readErr == nil {
				empty++
				if empty >= 100 {
					return io.ErrNoProgress
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
	}
	closeBody(nil)
	if closeErr != nil {
		return closeErr
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	keys := make([]string, 0, len(request.Trailer))
	for key := range request.Trailer {
		if !declared[strings.ToLower(key)] {
			return &MessageError{"request trailer name added during upload"}
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var trailers []qpack.DecodedField
	for _, key := range keys {
		name := strings.ToLower(key)
		for _, value := range request.Trailer[key] {
			trailers = append(trailers, qpack.DecodedField{Field: qpack.Field{Name: name, Value: value}, NeverIndex: name == "authorization" || name == "proxy-authorization" || name == "cookie"})
		}
	}
	if len(trailers) > 0 {
		if err := writer.Headers(trailers); err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	return writer.Close()
}
