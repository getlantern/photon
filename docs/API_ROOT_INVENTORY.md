# Exported API comparison: github.com/quic-go/quic-go

Generated from Go export data with `go run ./scripts/api-inventory -package=`. Reference version is pinned in interop/go.mod. Build tags and platform follow the current Go environment. This inventory includes resolved exported fields and promoted methods, including aliases. It does not prove behavioral compatibility. Declaration differences can include parameter names or internal type aliases and require manual review. Method entries distinguish value from pointer-only availability. Struct tags, interface type sets, generic constraints and constructor ownership require separate review.

| Reference symbol | Status | Reference declaration | Local declaration |
| --- | --- | --- | --- |
| `ApplicationError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `ApplicationError.ErrorCode (field)` | review difference | `internal/qerr.ApplicationErrorCode` | `internal/transport.ApplicationErrorCode` |
| `ApplicationError.ErrorMessage (field)` | same declaration | `string` | `string` |
| `ApplicationError.Is (method)` | same declaration | `func(target error) bool [pointer]` | `func(target error) bool [pointer]` |
| `ApplicationError.Remote (field)` | same declaration | `bool` | `bool` |
| `ApplicationError.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `ClientInfo.AddrVerified (field)` | same declaration | `bool` | `bool` |
| `ClientInfo.RemoteAddr (field)` | same declaration | `net.Addr` | `net.Addr` |
| `Config.Allow0RTT (field)` | same declaration | `bool` | `bool` |
| `Config.AllowConnectionWindowIncrease (field)` | same declaration | `func(conn *quic.Conn, delta uint64) bool` | `func(conn *quic.Conn, delta uint64) bool` |
| `Config.Clone (method)` | same declaration | `func() *quic.Config [pointer]` | `func() *quic.Config [pointer]` |
| `Config.DisablePathMTUDiscovery (field)` | same declaration | `bool` | `bool` |
| `Config.EnableDatagrams (field)` | same declaration | `bool` | `bool` |
| `Config.EnableStreamResetPartialDelivery (field)` | same declaration | `bool` | `bool` |
| `Config.GetConfigForClient (field)` | review difference | `func(info *quic.ClientInfo) (*quic.Config, error)` | `func(*quic.ClientInfo) (*quic.Config, error)` |
| `Config.HandshakeIdleTimeout (field)` | same declaration | `time.Duration` | `time.Duration` |
| `Config.InitialConnectionReceiveWindow (field)` | same declaration | `uint64` | `uint64` |
| `Config.InitialPacketSize (field)` | same declaration | `uint16` | `uint16` |
| `Config.InitialStreamReceiveWindow (field)` | same declaration | `uint64` | `uint64` |
| `Config.KeepAlivePeriod (field)` | same declaration | `time.Duration` | `time.Duration` |
| `Config.MaxConnectionReceiveWindow (field)` | same declaration | `uint64` | `uint64` |
| `Config.MaxIdleTimeout (field)` | same declaration | `time.Duration` | `time.Duration` |
| `Config.MaxIncomingStreams (field)` | same declaration | `int64` | `int64` |
| `Config.MaxIncomingUniStreams (field)` | same declaration | `int64` | `int64` |
| `Config.MaxStreamReceiveWindow (field)` | same declaration | `uint64` | `uint64` |
| `Config.RequireStreamCreditRelease (field)` | same declaration | `bool` | `bool` |
| `Config.TokenStore (field)` | same declaration | `quic.TokenStore` | `quic.TokenStore` |
| `Config.Tracer (field)` | missing | `func(ctx context.Context, isClient bool, connID quic.ConnectionID) qlogwriter.Trace` | `` |
| `Config.Versions (field)` | same declaration | `[]quic.Version` | `[]quic.Version` |
| `Conn.AcceptStream (method)` | same declaration | `func(ctx context.Context) (*quic.Stream, error) [pointer]` | `func(ctx context.Context) (*quic.Stream, error) [pointer]` |
| `Conn.AcceptUniStream (method)` | same declaration | `func(ctx context.Context) (*quic.ReceiveStream, error) [pointer]` | `func(ctx context.Context) (*quic.ReceiveStream, error) [pointer]` |
| `Conn.AddPath (method)` | same declaration | `func(t *quic.Transport) (*quic.Path, error) [pointer]` | `func(t *quic.Transport) (*quic.Path, error) [pointer]` |
| `Conn.CloseWithError (method)` | review difference | `func(code quic.ApplicationErrorCode, desc string) error [pointer]` | `func(code quic.ApplicationErrorCode, reason string) error [pointer]` |
| `Conn.ConnectionState (method)` | same declaration | `func() quic.ConnectionState [pointer]` | `func() quic.ConnectionState [pointer]` |
| `Conn.ConnectionStats (method)` | same declaration | `func() quic.ConnectionStats [pointer]` | `func() quic.ConnectionStats [pointer]` |
| `Conn.Context (method)` | same declaration | `func() context.Context [pointer]` | `func() context.Context [pointer]` |
| `Conn.HandshakeComplete (method)` | same declaration | `func() <-chan struct{} [pointer]` | `func() <-chan struct{} [pointer]` |
| `Conn.LocalAddr (method)` | same declaration | `func() net.Addr [pointer]` | `func() net.Addr [pointer]` |
| `Conn.NextConnection (method)` | same declaration | `func(ctx context.Context) (*quic.Conn, error) [pointer]` | `func(ctx context.Context) (*quic.Conn, error) [pointer]` |
| `Conn.OpenStream (method)` | same declaration | `func() (*quic.Stream, error) [pointer]` | `func() (*quic.Stream, error) [pointer]` |
| `Conn.OpenStreamSync (method)` | same declaration | `func(ctx context.Context) (*quic.Stream, error) [pointer]` | `func(ctx context.Context) (*quic.Stream, error) [pointer]` |
| `Conn.OpenUniStream (method)` | same declaration | `func() (*quic.SendStream, error) [pointer]` | `func() (*quic.SendStream, error) [pointer]` |
| `Conn.OpenUniStreamSync (method)` | same declaration | `func(ctx context.Context) (*quic.SendStream, error) [pointer]` | `func(ctx context.Context) (*quic.SendStream, error) [pointer]` |
| `Conn.QlogTrace (method)` | missing | `func() qlogwriter.Trace [pointer]` | `` |
| `Conn.ReceiveDatagram (method)` | same declaration | `func(ctx context.Context) ([]byte, error) [pointer]` | `func(ctx context.Context) ([]byte, error) [pointer]` |
| `Conn.RemoteAddr (method)` | same declaration | `func() net.Addr [pointer]` | `func() net.Addr [pointer]` |
| `Conn.SendDatagram (method)` | review difference | `func(p []byte) error [pointer]` | `func(data []byte) error [pointer]` |
| `ConnectionID.Bytes (method)` | same declaration | `func() []byte [value]` | `func() []byte [value]` |
| `ConnectionID.Len (method)` | same declaration | `func() int [value]` | `func() int [value]` |
| `ConnectionID.String (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `ConnectionID.Valid (method)` | same declaration | `func() bool [value]` | `func() bool [value]` |
| `ConnectionIDGenerator.ConnectionIDLen (method)` | same declaration | `func() int [value]` | `func() int [value]` |
| `ConnectionIDGenerator.GenerateConnectionID (method)` | same declaration | `func() (quic.ConnectionID, error) [value]` | `func() (quic.ConnectionID, error) [value]` |
| `ConnectionState.GSO (field)` | missing | `bool` | `` |
| `ConnectionState.RequireStreamCreditRelease (field)` | same declaration | `bool` | `bool` |
| `ConnectionState.SupportsDatagrams (field)` | review difference | `struct{Remote bool; Local bool}` | `bool` |
| `ConnectionState.SupportsStreamResetPartialDelivery (field)` | same declaration | `struct{Remote bool; Local bool}` | `struct{Remote bool; Local bool}` |
| `ConnectionState.TLS (field)` | same declaration | `crypto/tls.ConnectionState` | `crypto/tls.ConnectionState` |
| `ConnectionState.Used0RTT (field)` | same declaration | `bool` | `bool` |
| `ConnectionState.Version (field)` | same declaration | `quic.Version` | `quic.Version` |
| `ConnectionStats.BytesLost (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.BytesReceived (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.BytesSent (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.CongestionWindow (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.LatestRTT (field)` | same declaration | `time.Duration` | `time.Duration` |
| `ConnectionStats.MeanDeviation (field)` | same declaration | `time.Duration` | `time.Duration` |
| `ConnectionStats.MinRTT (field)` | same declaration | `time.Duration` | `time.Duration` |
| `ConnectionStats.PacketsLost (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.PacketsReceived (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.PacketsSent (field)` | same declaration | `uint64` | `uint64` |
| `ConnectionStats.SmoothedRTT (field)` | same declaration | `time.Duration` | `time.Duration` |
| `DatagramTooLargeError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `DatagramTooLargeError.Is (method)` | same declaration | `func(target error) bool [pointer]` | `func(target error) bool [pointer]` |
| `DatagramTooLargeError.MaxDatagramPayloadSize (field)` | same declaration | `int64` | `int64` |
| `EarlyListener.Accept (method)` | review difference | `func(ctx context.Context) (*quic.Conn, error) [pointer]` | `func(ctx context.Context) (*quic.Conn, error) [value]` |
| `EarlyListener.Addr (method)` | review difference | `func() net.Addr [pointer]` | `func() net.Addr [value]` |
| `EarlyListener.Close (method)` | review difference | `func() error [pointer]` | `func() error [value]` |
| `HandshakeTimeoutError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `HandshakeTimeoutError.Temporary (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `HandshakeTimeoutError.Timeout (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `HandshakeTimeoutError.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `IdleTimeoutError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `IdleTimeoutError.Temporary (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `IdleTimeoutError.Timeout (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `IdleTimeoutError.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Listener.Accept (method)` | same declaration | `func(ctx context.Context) (*quic.Conn, error) [pointer]` | `func(ctx context.Context) (*quic.Conn, error) [pointer]` |
| `Listener.Addr (method)` | same declaration | `func() net.Addr [pointer]` | `func() net.Addr [pointer]` |
| `Listener.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `OOBCapablePacketConn.Close (method)` | same declaration | `func() error [value]` | `func() error [value]` |
| `OOBCapablePacketConn.LocalAddr (method)` | same declaration | `func() net.Addr [value]` | `func() net.Addr [value]` |
| `OOBCapablePacketConn.ReadFrom (method)` | same declaration | `func(p []byte) (n int, addr net.Addr, err error) [value]` | `func(p []byte) (n int, addr net.Addr, err error) [value]` |
| `OOBCapablePacketConn.ReadMsgUDP (method)` | same declaration | `func(b []byte, oob []byte) (n int, oobn int, flags int, addr *net.UDPAddr, err error) [value]` | `func(b []byte, oob []byte) (n int, oobn int, flags int, addr *net.UDPAddr, err error) [value]` |
| `OOBCapablePacketConn.SetDeadline (method)` | same declaration | `func(t time.Time) error [value]` | `func(t time.Time) error [value]` |
| `OOBCapablePacketConn.SetReadBuffer (method)` | same declaration | `func(int) error [value]` | `func(int) error [value]` |
| `OOBCapablePacketConn.SetReadDeadline (method)` | same declaration | `func(t time.Time) error [value]` | `func(t time.Time) error [value]` |
| `OOBCapablePacketConn.SetWriteDeadline (method)` | same declaration | `func(t time.Time) error [value]` | `func(t time.Time) error [value]` |
| `OOBCapablePacketConn.SyscallConn (method)` | same declaration | `func() (syscall.RawConn, error) [value]` | `func() (syscall.RawConn, error) [value]` |
| `OOBCapablePacketConn.WriteMsgUDP (method)` | same declaration | `func(b []byte, oob []byte, addr *net.UDPAddr) (n int, oobn int, err error) [value]` | `func(b []byte, oob []byte, addr *net.UDPAddr) (n int, oobn int, err error) [value]` |
| `OOBCapablePacketConn.WriteTo (method)` | same declaration | `func(p []byte, addr net.Addr) (n int, err error) [value]` | `func(p []byte, addr net.Addr) (n int, err error) [value]` |
| `Path.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Path.Probe (method)` | same declaration | `func(ctx context.Context) error [pointer]` | `func(ctx context.Context) error [pointer]` |
| `Path.Switch (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `ReceiveStream.CancelRead (method)` | review difference | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(code quic.StreamErrorCode) [pointer]` |
| `ReceiveStream.Peek (method)` | review difference | `func(b []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [pointer]` |
| `ReceiveStream.Read (method)` | same declaration | `func(p []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [pointer]` |
| `ReceiveStream.ReleaseStreamCredit (method)` | same declaration | `func() [pointer]` | `func() [pointer]` |
| `ReceiveStream.SetReadDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `ReceiveStream.SetReceiveFinalSizeCallback (method)` | same declaration | `func(callback func(int64)) [pointer]` | `func(callback func(int64)) [pointer]` |
| `ReceiveStream.StreamID (method)` | review difference | `func() quic.StreamID [pointer]` | `func() quic.StreamID [value]` |
| `SendStream.CancelWrite (method)` | review difference | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(code quic.StreamErrorCode) [pointer]` |
| `SendStream.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `SendStream.Context (method)` | same declaration | `func() context.Context [pointer]` | `func() context.Context [pointer]` |
| `SendStream.SetPriority (method)` | same declaration | `func(urgency int8, incremental bool) [pointer]` | `func(urgency int8, incremental bool) [pointer]` |
| `SendStream.SetReliableBoundary (method)` | same declaration | `func() [pointer]` | `func() [pointer]` |
| `SendStream.SetWriteDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `SendStream.StreamID (method)` | review difference | `func() quic.StreamID [pointer]` | `func() quic.StreamID [value]` |
| `SendStream.TryWriteAll (method)` | same declaration | `func(p []byte) error [pointer]` | `func(p []byte) error [pointer]` |
| `SendStream.Write (method)` | same declaration | `func(p []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [pointer]` |
| `SendStream.WriteWithLimit (method)` | review difference | `func(p []byte, limiter func(maxBytes int) int) (int, error) [pointer]` | `func(p []byte, limiter func(int) int) (int, error) [pointer]` |
| `StatelessResetError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `StatelessResetError.Temporary (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `StatelessResetError.Timeout (method)` | same declaration | `func() bool [pointer]` | `func() bool [pointer]` |
| `StatelessResetError.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Stream.CancelRead (method)` | review difference | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(code quic.StreamErrorCode) [value]` |
| `Stream.CancelWrite (method)` | review difference | `func(errorCode quic.StreamErrorCode) [pointer]` | `func(code quic.StreamErrorCode) [value]` |
| `Stream.Close (method)` | review difference | `func() error [pointer]` | `func() error [value]` |
| `Stream.Context (method)` | review difference | `func() context.Context [pointer]` | `func() context.Context [value]` |
| `Stream.Peek (method)` | review difference | `func(b []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [value]` |
| `Stream.Read (method)` | review difference | `func(p []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [value]` |
| `Stream.ReleaseStreamCredit (method)` | same declaration | `func() [pointer]` | `func() [pointer]` |
| `Stream.SetDeadline (method)` | same declaration | `func(t time.Time) error [pointer]` | `func(t time.Time) error [pointer]` |
| `Stream.SetPriority (method)` | review difference | `func(urgency int8, incremental bool) [pointer]` | `func(urgency int8, incremental bool) [value]` |
| `Stream.SetReadDeadline (method)` | review difference | `func(t time.Time) error [pointer]` | `func(t time.Time) error [value]` |
| `Stream.SetReceiveFinalSizeCallback (method)` | review difference | `func(callback func(int64)) [pointer]` | `func(callback func(int64)) [value]` |
| `Stream.SetReliableBoundary (method)` | review difference | `func() [pointer]` | `func() [value]` |
| `Stream.SetWriteDeadline (method)` | review difference | `func(t time.Time) error [pointer]` | `func(t time.Time) error [value]` |
| `Stream.StreamID (method)` | same declaration | `func() quic.StreamID [pointer]` | `func() quic.StreamID [pointer]` |
| `Stream.TryWriteAll (method)` | review difference | `func(p []byte) error [pointer]` | `func(p []byte) error [value]` |
| `Stream.Write (method)` | review difference | `func(p []byte) (int, error) [pointer]` | `func(p []byte) (int, error) [value]` |
| `Stream.WriteWithLimit (method)` | review difference | `func(p []byte, limiter func(maxBytes int) int) (int, error) [pointer]` | `func(p []byte, limiter func(int) int) (int, error) [value]` |
| `StreamError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `StreamError.ErrorCode (field)` | review difference | `quic.StreamErrorCode` | `internal/transport.StreamErrorCode` |
| `StreamError.Is (method)` | same declaration | `func(target error) bool [pointer]` | `func(target error) bool [pointer]` |
| `StreamError.Remote (field)` | same declaration | `bool` | `bool` |
| `StreamError.StreamID (field)` | review difference | `quic.StreamID` | `internal/transport.StreamID` |
| `StreamID.StreamNum (method)` | review difference | `func() internal/protocol.StreamNum [value]` | `func() internal/transport.StreamNum [value]` |
| `StreamLimitReachedError.Error (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `TokenStore.Pop (method)` | review difference | `func(key string) (token *quic.ClientToken) [value]` | `func(key string) *quic.ClientToken [value]` |
| `TokenStore.Put (method)` | same declaration | `func(key string, token *quic.ClientToken) [value]` | `func(key string, token *quic.ClientToken) [value]` |
| `Transport.Close (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `Transport.Conn (field)` | same declaration | `net.PacketConn` | `net.PacketConn` |
| `Transport.ConnContext (field)` | same declaration | `func(context.Context, *quic.ClientInfo) (context.Context, error)` | `func(context.Context, *quic.ClientInfo) (context.Context, error)` |
| `Transport.ConnectionIDGenerator (field)` | same declaration | `quic.ConnectionIDGenerator` | `quic.ConnectionIDGenerator` |
| `Transport.ConnectionIDLength (field)` | same declaration | `int` | `int` |
| `Transport.Dial (method)` | same declaration | `func(ctx context.Context, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error) [pointer]` | `func(ctx context.Context, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error) [pointer]` |
| `Transport.DialEarly (method)` | same declaration | `func(ctx context.Context, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error) [pointer]` | `func(ctx context.Context, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error) [pointer]` |
| `Transport.DisableVersionNegotiationPackets (field)` | same declaration | `bool` | `bool` |
| `Transport.Listen (method)` | same declaration | `func(tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Listener, error) [pointer]` | `func(tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Listener, error) [pointer]` |
| `Transport.ListenEarly (method)` | same declaration | `func(tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.EarlyListener, error) [pointer]` | `func(tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.EarlyListener, error) [pointer]` |
| `Transport.MaxTokenAge (field)` | same declaration | `time.Duration` | `time.Duration` |
| `Transport.ReadNonQUICPacket (method)` | same declaration | `func(ctx context.Context, b []byte) (int, net.Addr, error) [pointer]` | `func(ctx context.Context, b []byte) (int, net.Addr, error) [pointer]` |
| `Transport.StatelessResetKey (field)` | same declaration | `*quic.StatelessResetKey` | `*quic.StatelessResetKey` |
| `Transport.TokenGeneratorKey (field)` | same declaration | `*quic.TokenGeneratorKey` | `*quic.TokenGeneratorKey` |
| `Transport.Tracer (field)` | missing | `qlogwriter.Recorder` | `` |
| `Transport.VerifySourceAddress (field)` | same declaration | `func(net.Addr) bool` | `func(net.Addr) bool` |
| `Transport.WriteTo (method)` | same declaration | `func(b []byte, addr net.Addr) (int, error) [pointer]` | `func(b []byte, addr net.Addr) (int, error) [pointer]` |
| `TransportError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `TransportError.ErrorCode (field)` | review difference | `internal/qerr.TransportErrorCode` | `internal/transport.TransportErrorCode` |
| `TransportError.ErrorMessage (field)` | same declaration | `string` | `string` |
| `TransportError.FrameType (field)` | same declaration | `uint64` | `uint64` |
| `TransportError.Is (method)` | same declaration | `func(target error) bool [pointer]` | `func(target error) bool [pointer]` |
| `TransportError.Remote (field)` | same declaration | `bool` | `bool` |
| `TransportError.Unwrap (method)` | same declaration | `func() []error [pointer]` | `func() []error [pointer]` |
| `TransportErrorCode.IsCryptoError (method)` | same declaration | `func() bool [value]` | `func() bool [value]` |
| `TransportErrorCode.Message (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `TransportErrorCode.String (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `Version.String (method)` | same declaration | `func() string [value]` | `func() string [value]` |
| `VersionNegotiationError.Error (method)` | same declaration | `func() string [pointer]` | `func() string [pointer]` |
| `VersionNegotiationError.Ours (field)` | review difference | `[]internal/protocol.Version` | `[]internal/wire.Version` |
| `VersionNegotiationError.Theirs (field)` | review difference | `[]internal/protocol.Version` | `[]internal/wire.Version` |
| `VersionNegotiationError.Unwrap (method)` | same declaration | `func() error [pointer]` | `func() error [pointer]` |
| `const AEADLimitReached` | review difference | `internal/qerr.TransportErrorCode = 15` | `quic.TransportErrorCode = 15` |
| `const ApplicationErrorErrorCode` | review difference | `internal/qerr.TransportErrorCode = 12` | `quic.TransportErrorCode = 12` |
| `const ConnectionIDLimitError` | review difference | `internal/qerr.TransportErrorCode = 9` | `quic.TransportErrorCode = 9` |
| `const ConnectionRefused` | review difference | `internal/qerr.TransportErrorCode = 2` | `quic.TransportErrorCode = 2` |
| `const CryptoBufferExceeded` | review difference | `internal/qerr.TransportErrorCode = 13` | `quic.TransportErrorCode = 13` |
| `const FinalSizeError` | review difference | `internal/qerr.TransportErrorCode = 6` | `quic.TransportErrorCode = 6` |
| `const FlowControlError` | review difference | `internal/qerr.TransportErrorCode = 3` | `quic.TransportErrorCode = 3` |
| `const FrameEncodingError` | review difference | `internal/qerr.TransportErrorCode = 7` | `quic.TransportErrorCode = 7` |
| `const InternalError` | review difference | `internal/qerr.TransportErrorCode = 1` | `quic.TransportErrorCode = 1` |
| `const InvalidToken` | review difference | `internal/qerr.TransportErrorCode = 11` | `quic.TransportErrorCode = 11` |
| `const KeyUpdateError` | review difference | `internal/qerr.TransportErrorCode = 14` | `quic.TransportErrorCode = 14` |
| `const NoError` | review difference | `internal/qerr.TransportErrorCode = 0` | `quic.TransportErrorCode = 0` |
| `const NoViablePathError` | review difference | `internal/qerr.TransportErrorCode = 16` | `quic.TransportErrorCode = 16` |
| `const ProtocolViolation` | review difference | `internal/qerr.TransportErrorCode = 10` | `quic.TransportErrorCode = 10` |
| `const StreamLimitError` | review difference | `internal/qerr.TransportErrorCode = 4` | `quic.TransportErrorCode = 4` |
| `const StreamStateError` | review difference | `internal/qerr.TransportErrorCode = 5` | `quic.TransportErrorCode = 5` |
| `const TransportParameterError` | review difference | `internal/qerr.TransportErrorCode = 8` | `quic.TransportErrorCode = 8` |
| `const Version1` | review difference | `internal/protocol.Version = 1` | `internal/wire.Version = 1` |
| `const Version2` | review difference | `internal/protocol.Version = 1798521807` | `internal/wire.Version = 1798521807` |
| `func ConnectionIDFromBytes` | same declaration | `func(b []byte) quic.ConnectionID` | `func(b []byte) quic.ConnectionID` |
| `func Dial` | same declaration | `func(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` | `func(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` |
| `func DialAddr` | review difference | `func(ctx context.Context, addr string, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` | `func(ctx context.Context, addr string, tlsConfig *crypto/tls.Config, config *quic.Config) (*quic.Conn, error)` |
| `func DialAddrEarly` | review difference | `func(ctx context.Context, addr string, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` | `func(ctx context.Context, addr string, tlsConfig *crypto/tls.Config, config *quic.Config) (*quic.Conn, error)` |
| `func DialEarly` | same declaration | `func(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` | `func(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *crypto/tls.Config, conf *quic.Config) (*quic.Conn, error)` |
| `func Listen` | same declaration | `func(conn net.PacketConn, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.Listener, error)` | `func(conn net.PacketConn, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.Listener, error)` |
| `func ListenAddr` | review difference | `func(addr string, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.Listener, error)` | `func(addr string, tlsConfig *crypto/tls.Config, config *quic.Config) (*quic.Listener, error)` |
| `func ListenAddrEarly` | review difference | `func(addr string, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.EarlyListener, error)` | `func(addr string, tlsConfig *crypto/tls.Config, config *quic.Config) (*quic.EarlyListener, error)` |
| `func ListenEarly` | same declaration | `func(conn net.PacketConn, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.EarlyListener, error)` | `func(conn net.PacketConn, tlsConf *crypto/tls.Config, config *quic.Config) (*quic.EarlyListener, error)` |
| `func NewLRUTokenStore` | same declaration | `func(maxOrigins int, tokensPerOrigin int) quic.TokenStore` | `func(maxOrigins int, tokensPerOrigin int) quic.TokenStore` |
| `func SupportedVersions` | same declaration | `func() []quic.Version` | `func() []quic.Version` |
| `type ApplicationError` | same declaration | `struct` | `struct` |
| `type ApplicationErrorCode` | same declaration | `uint64` | `uint64` |
| `type ClientInfo` | same declaration | `struct` | `struct` |
| `type ClientToken` | same declaration | `struct` | `struct` |
| `type Config` | same declaration | `struct` | `struct` |
| `type Conn` | same declaration | `struct` | `struct` |
| `type ConnectionID` | same declaration | `struct` | `struct` |
| `type ConnectionIDGenerator` | same declaration | `interface` | `interface` |
| `type ConnectionState` | same declaration | `struct` | `struct` |
| `type ConnectionStats` | same declaration | `struct` | `struct` |
| `type DatagramTooLargeError` | same declaration | `struct` | `struct` |
| `type EarlyListener` | same declaration | `struct` | `struct` |
| `type HandshakeTimeoutError` | same declaration | `struct` | `struct` |
| `type IdleTimeoutError` | same declaration | `struct` | `struct` |
| `type Listener` | same declaration | `struct` | `struct` |
| `type OOBCapablePacketConn` | same declaration | `interface` | `interface` |
| `type Path` | same declaration | `struct` | `struct` |
| `type ReceiveStream` | same declaration | `struct` | `struct` |
| `type SendStream` | same declaration | `struct` | `struct` |
| `type StatelessResetError` | same declaration | `struct` | `struct` |
| `type StatelessResetKey` | same declaration | `[32]byte` | `[32]byte` |
| `type Stream` | same declaration | `struct` | `struct` |
| `type StreamError` | same declaration | `struct` | `struct` |
| `type StreamErrorCode` | same declaration | `uint64` | `uint64` |
| `type StreamID` | same declaration | `int64` | `int64` |
| `type StreamLimitReachedError` | same declaration | `struct` | `struct` |
| `type TokenGeneratorKey` | same declaration | `[32]byte` | `[32]byte` |
| `type TokenStore` | same declaration | `interface` | `interface` |
| `type Transport` | same declaration | `struct` | `struct` |
| `type TransportError` | same declaration | `struct` | `struct` |
| `type TransportErrorCode` | same declaration | `uint64` | `uint64` |
| `type Version` | same declaration | `uint32` | `uint32` |
| `type VersionNegotiationError` | same declaration | `struct` | `struct` |
| `var Err0RTTRejected` | same declaration | `error` | `error` |
| `var ErrPathClosed` | same declaration | `error` | `error` |
| `var ErrPathNotValidated` | same declaration | `error` | `error` |
| `var ErrServerClosed` | same declaration | `quic.errServerClosed` | `quic.errServerClosed` |
| `var ErrStreamClosed` | same declaration | `error` | `error` |
| `var ErrTransportClosed` | same declaration | `*quic.errTransportClosed` | `*quic.errTransportClosed` |
| `var ErrWouldBlock` | same declaration | `error` | `error` |
| `var ErrWriteLimitReached` | same declaration | `error` | `error` |
| `var QUICVersionContextKey` | same declaration | `*internal/handshake.quicVersionContextKey` | `*internal/handshake.quicVersionContextKey` |

Reference entries: 253; missing: 4; declaration differences: 58; same declarations: 191.
Local-only entries are excluded; their presence does not compensate for missing reference capabilities.
