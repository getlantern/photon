# Exported API comparison: github.com/quic-go/quic-go/http3

Generated from Go export data with `go run ./scripts/api-inventory -package=http3`. Reference version is pinned in interop/go.mod. Build tags and platform follow the current Go environment. This inventory includes resolved exported fields and promoted methods, including aliases. It does not prove behavioral compatibility. Declaration differences can include parameter names or internal type aliases and require manual review. Method entries distinguish value from pointer-only availability. Struct tags, interface type sets, generic constraints and constructor ownership require separate review.

| Reference symbol | Status | Reference declaration | Local declaration |
| --- | --- | --- | --- |
| `CapsuleParser.Next (method)` | same declaration | `func() (http3.CapsuleType, http3.CapsuleReader, error) [pointer]` | `func() (http3.CapsuleType, http3.CapsuleReader, error) [pointer]` |
| `CapsuleReader.Discard (method)` | same declaration | `func() error [value]` | `func() error [value]` |
| `CapsuleReader.Read (method)` | same declaration | `func(b []byte) (int, error) [value]` | `func(b []byte) (int, error) [value]` |
| `CapsuleReader.ReadByte (method)` | same declaration | `func() (byte, error) [value]` | `func() (byte, error) [value]` |
| `CapsuleReader.Remaining (method)` | same declaration | `func() int64 [value]` | `func() int64 [value]` |
| `ClientConn.CloseWithError (method)` | same declaration | `func(code quic.ApplicationErrorCode, msg string) error [pointer]` | `func(code quic.ApplicationErrorCode, msg string) error [pointer]` |
| `ClientConn.Context (method)` | same declaration | `func() context.Context [pointer]` | `func() context.Context [pointer]` |
| `ClientConn.HandleBidirectionalStream (method)` | same declaration | `func(str *quic.Stream) [pointer]` | `func(str *quic.Stream) [pointer]` |
| `ClientConn.LocalAddr (method)` | same declaration | `func() net.Addr [pointer]` | `func() net.Addr [pointer]` |
| `ClientConn.OpenRequestStream (method)` | same declaration | `func(ctx context.Context) (*http3.RequestStream, error) [pointer]` | `func(ctx context.Context) (*http3.RequestStream, error) [pointer]` |
| `ClientConn.ReceivedSettings (method)` | same declaration | `func() <-chan struct{} [pointer]` | `func() <-chan struct{} [pointer]` |
| `ClientConn.RemoteAddr (method)` | same declaration | `func() net.Addr [pointer]` | `func() net.Addr [pointer]` |
| `ClientConn.RoundTrip (method)` | same declaration | `func(req *net/http.Request) (*net/http.Response, error) [pointer]` | `func(req *net/http.Request) (*net/http.Response, error) [pointer]` |
| `ClientConn.Settings (method)` | same declaration | `func() *http3.Settings [pointer]` | `func() *http3.Settings [pointer]` |
| `ErrCode.String (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `Error.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `Error.ErrorCode (field)` | same declaration | `http3.ErrCode` | `http3.ErrCode` |
| `Error.ErrorMessage (field)` | same declaration | `string` | `string` |
| `Error.Is (method)` | same declaration | `func(target error) bool [pointer]` | `func(target error) bool [pointer]` |
| `Error.Remote (field)` | same declaration | `bool` | `bool` |
| `Error.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `HTTPStreamer.HTTPStream (method)` | same declaration | `func() (*http3.Stream, error) [value]` | `func() (*http3.Stream, error) [value]` |
| `QUICListener.Accept (method)` | same declaration | `func(context.Context) (*quic.Conn, error) [value]` | `func(context.Context) (*quic.Conn, error) [value]` |
| `QUICListener.Addr (method)` | same declaration | `func() net.Addr [value]` | `func() net.Addr [value]` |
| `QUICListener.Close (method)` | same declaration | `func() error [value]` | `func() error [value]` |
| `RawClientConn.ClientConn (field)` | same declaration | `*http3.ClientConn` | `*http3.ClientConn` |
| `RawClientConn.CloseWithError (method)` | same declaration | `func(code quic.ApplicationErrorCode, msg string) error [value]` | `func(code quic.ApplicationErrorCode, msg string) error [value]` |
| `RawClientConn.Context (method)` | same declaration | `func() context.Context [value]` | `func() context.Context [value]` |
| `RawClientConn.HandleBidirectionalStream (method)` | same declaration | `func(str *quic.Stream) [value]` | `func(str *quic.Stream) [value]` |
| `RawClientConn.HandleUnidirectionalStream (method)` | same declaration | `func(str *quic.ReceiveStream) [pointer]` | `func(str *quic.ReceiveStream) [pointer]` |
| `RawClientConn.LocalAddr (method)` | same declaration | `func() net.Addr [value]` | `func() net.Addr [value]` |
| `RawClientConn.OpenRequestStream (method)` | same declaration | `func(ctx context.Context) (*http3.RequestStream, error) [value]` | `func(ctx context.Context) (*http3.RequestStream, error) [value]` |
| `RawClientConn.ReceivedSettings (method)` | same declaration | `func() <-chan struct{} [value]` | `func() <-chan struct{} [value]` |
| `RawClientConn.RemoteAddr (method)` | same declaration | `func() net.Addr [value]` | `func() net.Addr [value]` |
| `RawClientConn.RoundTrip (method)` | same declaration | `func(req *net/http.Request) (*net/http.Response, error) [value]` | `func(req *net/http.Request) (*net/http.Response, error) [value]` |
| `RawClientConn.Settings (method)` | same declaration | `func() *http3.Settings [value]` | `func() *http3.Settings [value]` |
| `RawServerConn.CloseWithError (method)` | same declaration | `func(code quic.ApplicationErrorCode, msg string) error [pointer]` | `func(code quic.ApplicationErrorCode, msg string) error [pointer]` |
| `RawServerConn.HandleRequestStream (method)` | same declaration | `func(str *quic.Stream) [pointer]` | `func(str *quic.Stream) [pointer]` |
| `RawServerConn.HandleUnidirectionalStream (method)` | same declaration | `func(str *quic.ReceiveStream) [pointer]` | `func(str *quic.ReceiveStream) [pointer]` |
| `RequestStream.CancelRead (method)` | same declaration | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(errorCode quic.StreamErrorCode) [pointer]` |
| `RequestStream.CancelWrite (method)` | same declaration | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(errorCode quic.StreamErrorCode) [pointer]` |
| `RequestStream.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `RequestStream.Context (method)` | same declaration | `func() context.Context [pointer]` | `func() context.Context [pointer]` |
| `RequestStream.QlogTrace (method)` | missing | `func() qlogwriter.Trace [pointer]` | `` |
| `RequestStream.Read (method)` | same declaration | `func(b []byte) (int, error) [pointer]` | `func(b []byte) (int, error) [pointer]` |
| `RequestStream.ReadResponse (method)` | same declaration | `func() (*net/http.Response, error) [pointer]` | `func() (*net/http.Response, error) [pointer]` |
| `RequestStream.ReceiveDatagram (method)` | same declaration | `func(ctx context.Context) ([]byte, error) [pointer]` | `func(ctx context.Context) ([]byte, error) [pointer]` |
| `RequestStream.SendDatagram (method)` | same declaration | `func(b []byte) error [pointer]` | `func(b []byte) error [pointer]` |
| `RequestStream.SendRequestHeader (method)` | same declaration | `func(req *net/http.Request) error [pointer]` | `func(req *net/http.Request) error [pointer]` |
| `RequestStream.SetDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `RequestStream.SetReadDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `RequestStream.SetWriteDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `RequestStream.StreamID (method)` | same declaration | `func() quic.StreamID [pointer]` | `func() quic.StreamID [pointer]` |
| `RequestStream.TryWriteAll (method)` | same declaration | `func(b []byte) error [pointer]` | `func(b []byte) error [pointer]` |
| `RequestStream.Write (method)` | same declaration | `func(b []byte) (int, error) [pointer]` | `func(b []byte) (int, error) [pointer]` |
| `RoundTripOpt.OnlyCachedConn (field)` | same declaration | `bool` | `bool` |
| `Server.AdditionalSettings (field)` | same declaration | `map[uint64]uint64` | `map[uint64]uint64` |
| `Server.Addr (field)` | same declaration | `string` | `string` |
| `Server.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Server.ConnContext (field)` | same declaration | `func(ctx context.Context, c *quic.Conn) context.Context` | `func(ctx context.Context, c *quic.Conn) context.Context` |
| `Server.EnableDatagrams (field)` | same declaration | `bool` | `bool` |
| `Server.Handler (field)` | same declaration | `net/http.Handler` | `net/http.Handler` |
| `Server.IdleTimeout (field)` | same declaration | `time.Duration` | `time.Duration` |
| `Server.ListenAndServe (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Server.ListenAndServeTLS (method)` | same declaration | `func(certFile string, keyFile string) error [pointer]` | `func(certFile string, keyFile string) error [pointer]` |
| `Server.Logger (field)` | same declaration | `*log/slog.Logger` | `*log/slog.Logger` |
| `Server.MaxHeaderBytes (field)` | same declaration | `int` | `int` |
| `Server.NewRawServerConn (method)` | same declaration | `func(conn *quic.Conn) (*http3.RawServerConn, error) [pointer]` | `func(conn *quic.Conn) (*http3.RawServerConn, error) [pointer]` |
| `Server.Port (field)` | same declaration | `int` | `int` |
| `Server.QUICConfig (field)` | same declaration | `*quic.Config` | `*quic.Config` |
| `Server.Serve (method)` | review difference | `func(conn net.PacketConn) error [pointer]` | `func(parent context.Context, listener *quic.Listener) error [pointer]` |
| `Server.ServeListener (method)` | review difference | `func(ln http3.QUICListener) error [pointer]` | `func(listener http3.QUICListener) error [pointer]` |
| `Server.ServeQUICConn (method)` | review difference | `func(conn *quic.Conn) error [pointer]` | `func(parent context.Context, conn *quic.Conn) (err error) [pointer]` |
| `Server.SetQUICHeaders (method)` | same declaration | `func(hdr net/http.Header) error [pointer]` | `func(hdr net/http.Header) error [pointer]` |
| `Server.Shutdown (method)` | same declaration | `func(ctx context.Context) error [pointer]` | `func(ctx context.Context) error [pointer]` |
| `Server.TLSConfig (field)` | same declaration | `*crypto/tls.Config` | `*crypto/tls.Config` |
| `Settings.EnableDatagrams (field)` | same declaration | `bool` | `bool` |
| `Settings.EnableExtendedConnect (field)` | same declaration | `bool` | `bool` |
| `Settings.Other (field)` | same declaration | `map[uint64]uint64` | `map[uint64]uint64` |
| `Settingser.ReceivedSettings (method)` | same declaration | `func() <-chan struct{} [value]` | `func() <-chan struct{} [value]` |
| `Settingser.Settings (method)` | same declaration | `func() *http3.Settings [value]` | `func() *http3.Settings [value]` |
| `Stream.CancelRead (method)` | same declaration | `func(quic.StreamErrorCode) [value]` | `func(quic.StreamErrorCode) [value]` |
| `Stream.CancelWrite (method)` | same declaration | `func(quic.StreamErrorCode) [value]` | `func(quic.StreamErrorCode) [value]` |
| `Stream.Close (method)` | same declaration | `func() error [value]` | `func() error [value]` |
| `Stream.Context (method)` | same declaration | `func() context.Context [value]` | `func() context.Context [value]` |
| `Stream.QUICStream (method)` | same declaration | `func() *quic.Stream [value]` | `func() *quic.Stream [value]` |
| `Stream.QlogTrace (method)` | missing | `func() qlogwriter.Trace [pointer]` | `` |
| `Stream.Read (method)` | same declaration | `func(b []byte) (int, error) [pointer]` | `func(b []byte) (int, error) [pointer]` |
| `Stream.ReceiveDatagram (method)` | same declaration | `func(ctx context.Context) ([]byte, error) [pointer]` | `func(ctx context.Context) ([]byte, error) [pointer]` |
| `Stream.SendDatagram (method)` | same declaration | `func(b []byte) error [pointer]` | `func(b []byte) error [pointer]` |
| `Stream.SetDeadline (method)` | same declaration | `func(time.Time) error [value]` | `func(time.Time) error [value]` |
| `Stream.SetPriority (method)` | same declaration | `func(urgency int8, incremental bool) [value]` | `func(urgency int8, incremental bool) [value]` |
| `Stream.SetReadDeadline (method)` | same declaration | `func(time.Time) error [value]` | `func(time.Time) error [value]` |
| `Stream.SetWriteDeadline (method)` | same declaration | `func(time.Time) error [value]` | `func(time.Time) error [value]` |
| `Stream.StreamID (method)` | same declaration | `func() quic.StreamID [pointer]` | `func() quic.StreamID [pointer]` |
| `Stream.TryWriteAll (method)` | same declaration | `func(b []byte) error [pointer]` | `func(b []byte) error [pointer]` |
| `Stream.Write (method)` | same declaration | `func(b []byte) (int, error) [pointer]` | `func(b []byte) (int, error) [pointer]` |
| `Transport.AdditionalSettings (field)` | same declaration | `map[uint64]uint64` | `map[uint64]uint64` |
| `Transport.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Transport.CloseIdleConnections (method)` | same declaration | `func() [pointer]` | `func() [pointer]` |
| `Transport.Dial (field)` | same declaration | `func(ctx context.Context, addr string, tlsCfg *crypto/tls.Config, cfg *quic.Config) (*quic.Conn, error)` | `func(ctx context.Context, addr string, tlsCfg *crypto/tls.Config, cfg *quic.Config) (*quic.Conn, error)` |
| `Transport.DisableCompression (field)` | same declaration | `bool` | `bool` |
| `Transport.EnableDatagrams (field)` | same declaration | `bool` | `bool` |
| `Transport.Logger (field)` | same declaration | `*log/slog.Logger` | `*log/slog.Logger` |
| `Transport.MaxResponseHeaderBytes (field)` | same declaration | `int` | `int` |
| `Transport.NewClientConn (method)` | same declaration | `func(conn *quic.Conn) *http3.ClientConn [pointer]` | `func(conn *quic.Conn) *http3.ClientConn [pointer]` |
| `Transport.NewRawClientConn (method)` | same declaration | `func(conn *quic.Conn) *http3.RawClientConn [pointer]` | `func(conn *quic.Conn) *http3.RawClientConn [pointer]` |
| `Transport.QUICConfig (field)` | same declaration | `*quic.Config` | `*quic.Config` |
| `Transport.RoundTrip (method)` | review difference | `func(req *net/http.Request) (*net/http.Response, error) [pointer]` | `func(request *net/http.Request) (*net/http.Response, error) [pointer]` |
| `Transport.RoundTripOpt (method)` | review difference | `func(req *net/http.Request, opt http3.RoundTripOpt) (*net/http.Response, error) [pointer]` | `func(request *net/http.Request, opt http3.RoundTripOpt) (response *net/http.Response, err error) [pointer]` |
| `Transport.TLSClientConfig (field)` | same declaration | `*crypto/tls.Config` | `*crypto/tls.Config` |
| `const CapsuleProtocolHeader` | same declaration | `untyped string = "Capsule-Protocol"` | `untyped string = "Capsule-Protocol"` |
| `const ErrCodeClosedCriticalStream` | same declaration | `http3.ErrCode = 260` | `http3.ErrCode = 260` |
| `const ErrCodeConnectError` | same declaration | `http3.ErrCode = 271` | `http3.ErrCode = 271` |
| `const ErrCodeDatagramError` | same declaration | `http3.ErrCode = 51` | `http3.ErrCode = 51` |
| `const ErrCodeExcessiveLoad` | same declaration | `http3.ErrCode = 263` | `http3.ErrCode = 263` |
| `const ErrCodeFrameError` | same declaration | `http3.ErrCode = 262` | `http3.ErrCode = 262` |
| `const ErrCodeFrameUnexpected` | same declaration | `http3.ErrCode = 261` | `http3.ErrCode = 261` |
| `const ErrCodeGeneralProtocolError` | same declaration | `http3.ErrCode = 257` | `http3.ErrCode = 257` |
| `const ErrCodeIDError` | same declaration | `http3.ErrCode = 264` | `http3.ErrCode = 264` |
| `const ErrCodeInternalError` | same declaration | `http3.ErrCode = 258` | `http3.ErrCode = 258` |
| `const ErrCodeMessageError` | same declaration | `http3.ErrCode = 270` | `http3.ErrCode = 270` |
| `const ErrCodeMissingSettings` | same declaration | `http3.ErrCode = 266` | `http3.ErrCode = 266` |
| `const ErrCodeNoError` | same declaration | `http3.ErrCode = 256` | `http3.ErrCode = 256` |
| `const ErrCodeQPACKDecompressionFailed` | same declaration | `http3.ErrCode = 512` | `http3.ErrCode = 512` |
| `const ErrCodeRequestCanceled` | same declaration | `http3.ErrCode = 268` | `http3.ErrCode = 268` |
| `const ErrCodeRequestIncomplete` | same declaration | `http3.ErrCode = 269` | `http3.ErrCode = 269` |
| `const ErrCodeRequestRejected` | same declaration | `http3.ErrCode = 267` | `http3.ErrCode = 267` |
| `const ErrCodeSettingsError` | same declaration | `http3.ErrCode = 265` | `http3.ErrCode = 265` |
| `const ErrCodeStreamCreationError` | same declaration | `http3.ErrCode = 259` | `http3.ErrCode = 259` |
| `const ErrCodeVersionFallback` | same declaration | `http3.ErrCode = 272` | `http3.ErrCode = 272` |
| `const MethodGet0RTT` | same declaration | `untyped string = "GET_0RTT"` | `untyped string = "GET_0RTT"` |
| `const MethodHead0RTT` | same declaration | `untyped string = "HEAD_0RTT"` | `untyped string = "HEAD_0RTT"` |
| `const NextProtoH3` | same declaration | `untyped string = "h3"` | `untyped string = "h3"` |
| `func ConfigureTLSConfig` | review difference | `func(tlsConf *crypto/tls.Config) *crypto/tls.Config` | `func(source *crypto/tls.Config) *crypto/tls.Config` |
| `func ListenAndServeQUIC` | same declaration | `func(addr string, certFile string, keyFile string, handler net/http.Handler) error` | `func(addr string, certFile string, keyFile string, handler net/http.Handler) error` |
| `func ListenAndServeTLS` | same declaration | `func(addr string, certFile string, keyFile string, handler net/http.Handler) error` | `func(addr string, certFile string, keyFile string, handler net/http.Handler) error` |
| `func NewCapsuleParser` | same declaration | `func(r io.Reader) *http3.CapsuleParser` | `func(r io.Reader) *http3.CapsuleParser` |
| `func WriteCapsule` | review difference | `func(w quicvarint.Writer, ct http3.CapsuleType, value []byte) error` | `func(w io.Writer, typ http3.CapsuleType, value []byte) error` |
| `type CapsuleParser` | same declaration | `struct` | `struct` |
| `type CapsuleReader` | same declaration | `struct` | `struct` |
| `type CapsuleType` | same declaration | `uint64` | `uint64` |
| `type ClientConn` | same declaration | `struct` | `struct` |
| `type ErrCode` | same declaration | `uint64` | `uint64` |
| `type Error` | same declaration | `struct` | `struct` |
| `type FrameType` | same declaration | `uint64` | `uint64` |
| `type HTTPStreamer` | same declaration | `interface` | `interface` |
| `type QUICListener` | same declaration | `interface` | `interface` |
| `type RawClientConn` | same declaration | `struct` | `struct` |
| `type RawServerConn` | same declaration | `struct` | `struct` |
| `type RequestStream` | same declaration | `struct` | `struct` |
| `type RoundTripOpt` | same declaration | `struct` | `struct` |
| `type Server` | same declaration | `struct` | `struct` |
| `type Settings` | same declaration | `struct` | `struct` |
| `type Settingser` | same declaration | `interface` | `interface` |
| `type Stream` | same declaration | `struct` | `struct` |
| `type StreamType` | same declaration | `uint64` | `uint64` |
| `type Transport` | same declaration | `struct` | `struct` |
| `var ErrNoAltSvcPort` | same declaration | `error` | `error` |
| `var ErrNoCachedConn` | same declaration | `error` | `error` |
| `var ErrTransportClosed` | same declaration | `error` | `error` |
| `var RemoteAddrContextKey` | same declaration | `*http3.contextKey` | `*http3.contextKey` |
| `var ServerContextKey` | same declaration | `*http3.contextKey` | `*http3.contextKey` |

Reference entries: 163; missing: 2; declaration differences: 7; same declarations: 154.
Local-only entries are excluded; their presence does not compensate for missing reference capabilities.
