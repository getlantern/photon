// Command pathbench measures how QUIC senders share a real network path.
//
// A server listens on three UDP ports, one per sender: photon with BBR,
// photon with NewReno, and quic-go (whose v0.65.0 sender is Reno, though
// built by its Cubic code). A client runs a mix of
// concurrent downloads from them over one path, each its own connection,
// and reports each flow's goodput, share and RTT over a measurement window.
// The server logs each connection's sender-side statistics when it closes.
//
//	pathbench server -cert-out cert.pem -addr 0.0.0.0 [-port 4433]
//	pathbench client -server host -cert cert.pem [-port 4433] \
//	    -flows photon-bbr:1,quic-go:1 [-duration 40s] [-measure-from 10s] \
//	    [-stagger 2s] [-json]
//
// Only the server's congestion control paces the data: the client is
// quic-go throughout, and it sends nothing but a short request per flow.
// The server answers anyone who connects, with one endless download per
// connection, so run it only for a measurement and on hosts made for it.
// The server's certificate is self-signed for the name "pathbench" and
// written to -cert-out; the client trusts exactly that certificate (-cert),
// so copy the file from server to client.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

const alpn = "pathbench"

// senders are the server's senders, in port order from the base port.
var senders = []string{"photon-bbr", "photon-newreno", "quic-go"}

// The client's receive windows, large enough not to limit a long, fast path
// (64 MiB covers 1 Gbit/s at 500 ms). Each flow is one stream on its own
// connection, so the stream window is the one that binds. The server
// receives only a request.
const (
	streamWindow     = 64 << 20
	connectionWindow = 64 << 20
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pathbench server|client [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "server":
		err = serverMain(os.Args[2:])
	case "client":
		err = clientMain(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

// --- Server ---

func serverMain(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1", "address to listen on; 0.0.0.0 to serve remote clients")
	port := fs.Int("port", 4433, "base UDP port: photon-bbr, then photon-newreno, then quic-go")
	certOut := fs.String("cert-out", "pathbench-cert.pem", "where to write the server's certificate, for clients' -cert")
	fs.Parse(args)
	if *port < 1 || *port > 65535-len(senders)+1 {
		return fmt.Errorf("-port %d leaves no room for %d consecutive ports", *port, len(senders))
	}
	cert, err := selfSigned()
	if err != nil {
		return err
	}
	if err := os.WriteFile(*certOut, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o644); err != nil {
		return err
	}
	_, addrs, err := serve(context.Background(), *addr, *port, cert, os.Stdout)
	if err != nil {
		return err
	}
	for _, a := range addrs {
		log.Print(a)
	}
	select {} // until killed
}

// serve starts the three senders' listeners and returns a function that
// stops them. Each closed connection's statistics go to out as a JSON line.
func serve(ctx context.Context, addr string, port int, cert tls.Certificate, out io.Writer) (stop func(), addrs []string, err error) {
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{alpn}}
	var mu sync.Mutex
	logStats := func(v any) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(out).Encode(v)
	}
	ctx, cancel := context.WithCancel(ctx)
	var closers []func()
	stop = func() {
		cancel()
		for _, c := range closers {
			c()
		}
	}
	for i, name := range senders {
		listenAddr := net.JoinHostPort(addr, strconv.Itoa(port+i))
		if name == "quic-go" {
			l, err := upstream.ListenAddr(listenAddr, tlsConfig, &upstream.Config{MaxIdleTimeout: 30 * time.Second})
			if err != nil {
				stop()
				return nil, nil, err
			}
			closers = append(closers, func() { l.Close() })
			addrs = append(addrs, name+" on "+l.Addr().String())
			go acceptUpstream(ctx, l, name, logStats)
			continue
		}
		congestion := quic.CongestionControlBBR
		if name == "photon-newreno" {
			congestion = quic.CongestionControlNewReno
		}
		l, err := quic.ListenAddr(listenAddr, tlsConfig, &quic.Config{MaxIdleTimeout: 30 * time.Second, CongestionControl: congestion})
		if err != nil {
			stop()
			return nil, nil, err
		}
		closers = append(closers, func() { l.Close() })
		addrs = append(addrs, name+" on "+l.Addr().String())
		go acceptPhoton(ctx, l, name, logStats)
	}
	return stop, addrs, nil
}

// connStats is the line the server logs for each connection.
type connStats struct {
	Sender      string  `json:"sender"`
	Remote      string  `json:"remote"`
	Seconds     float64 `json:"seconds"`
	BytesSent   uint64  `json:"bytes_sent"`
	PacketsSent uint64  `json:"packets_sent"`
	BytesLost   uint64  `json:"bytes_lost"`
	PacketsLost uint64  `json:"packets_lost"`
	LossPercent float64 `json:"loss_percent"`
	MinRTTms    float64 `json:"min_rtt_ms,omitempty"` // left out if not sampled
	SRTTms      float64 `json:"srtt_ms,omitempty"`    // left out if not sampled
	CwndBytes   *uint64 `json:"cwnd_bytes,omitempty"`
}

func newConnStats(sender string, remote net.Addr, start time.Time, sent, packetsSent, lost, packetsLost uint64, minRTT, srtt time.Duration, cwnd *uint64) connStats {
	s := connStats{Sender: sender, Remote: remote.String(), Seconds: time.Since(start).Seconds(), BytesSent: sent, PacketsSent: packetsSent, BytesLost: lost, PacketsLost: packetsLost, MinRTTms: ms(minRTT), SRTTms: ms(srtt), CwndBytes: cwnd}
	if packetsSent > 0 {
		s.LossPercent = 100 * float64(packetsLost) / float64(packetsSent)
	}
	return s
}

func acceptPhoton(ctx context.Context, l *quic.Listener, name string, logStats func(any)) {
	for {
		c, err := l.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			start := time.Now()
			var last quic.ConnectionStats
			done := make(chan struct{})
			go func() {
				// Statistics freeze once the connection closes; keep the latest.
				for {
					select {
					case <-c.Context().Done():
						close(done)
						return
					case <-time.After(200 * time.Millisecond):
						last = c.ConnectionStats()
					}
				}
			}()
			// One download per connection.
			if s, err := c.AcceptStream(c.Context()); err == nil {
				go sendForever(s)
			}
			<-done
			if final := c.ConnectionStats(); final.BytesSent >= last.BytesSent {
				last = final
			}
			cwnd := last.CongestionWindow
			logStats(newConnStats(name, c.RemoteAddr(), start, last.BytesSent, last.PacketsSent, last.BytesLost, last.PacketsLost, last.MinRTT, last.SmoothedRTT, &cwnd))
		}()
	}
}

func acceptUpstream(ctx context.Context, l *upstream.Listener, name string, logStats func(any)) {
	for {
		c, err := l.Accept(ctx)
		if err != nil {
			return
		}
		go func() {
			start := time.Now()
			// One download per connection.
			if s, err := c.AcceptStream(c.Context()); err == nil {
				go sendForever(s)
			}
			<-c.Context().Done()
			st := c.ConnectionStats()
			cwnd := st.CongestionWindow
			logStats(newConnStats(name, c.RemoteAddr(), start, st.BytesSent, st.PacketsSent, st.BytesLost, st.PacketsLost, st.MinRTT, st.SmoothedRTT, &cwnd))
		}()
	}
}

// responseDelay holds each response back, for tests of late flows.
var responseDelay time.Duration

// sendForever reads the client's request, then writes until the stream or
// connection fails.
func sendForever(s io.ReadWriteCloser) {
	io.Copy(io.Discard, s)
	time.Sleep(responseDelay)
	chunk := make([]byte, 64<<10)
	for {
		if _, err := s.Write(chunk); err != nil {
			return
		}
	}
}

func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"pathbench"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(30 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// --- Client ---

// flowResult is one flow's measurement. Goodput, share and mean smoothed
// RTT cover the measurement window; the minimum RTT is the connection's
// lifetime minimum, handshake included, as quic-go reports it.
type flowResult struct {
	Flow         int     `json:"flow"`
	Sender       string  `json:"sender"`
	StartSeconds float64 `json:"start_seconds"`
	Mbps         float64 `json:"mbps"`
	SharePercent float64 `json:"share_percent"`
	MinRTTms     float64 `json:"min_rtt_ms,omitempty"`   // lifetime; left out if not sampled
	MeanSRTTms   float64 `json:"mean_srtt_ms,omitempty"` // left out if not sampled
	Error        string  `json:"error,omitempty"`
}

// runResult is a whole run's measurement.
type runResult struct {
	Server        string       `json:"server"`
	Flows         []flowResult `json:"flows"`
	TotalMbps     float64      `json:"total_mbps"`
	WindowSeconds float64      `json:"window_seconds"`
}

// clientConfig is what one client run measures.
type clientConfig struct {
	server           string
	port             int
	mix              []string // sender per flow, in start order
	duration, from   time.Duration
	stagger          time.Duration
	handshakeTimeout time.Duration
	roots            *x509.CertPool // trusts the server's certificate
}

func clientMain(args []string) error {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	server := fs.String("server", "", "server host")
	certFile := fs.String("cert", "pathbench-cert.pem", "the server's certificate, from its -cert-out")
	port := fs.Int("port", 4433, "server's base UDP port")
	flows := fs.String("flows", "photon-bbr:1", "flows per sender, comma-separated sender:count; senders: "+strings.Join(senders, ", "))
	duration := fs.Duration("duration", 40*time.Second, "run length")
	from := fs.Duration("measure-from", 10*time.Second, "start of the measurement window")
	stagger := fs.Duration("stagger", 2*time.Second, "delay between flow starts")
	asJSON := fs.Bool("json", false, "print JSON")
	fs.Parse(args)
	if *server == "" {
		return errors.New("-server is required")
	}
	if *port < 1 || *port > 65535-len(senders)+1 {
		return fmt.Errorf("-port %d leaves no room for %d consecutive ports", *port, len(senders))
	}
	mix, err := parseFlows(*flows)
	if err != nil {
		return err
	}
	pemBytes, err := os.ReadFile(*certFile)
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pemBytes) {
		return fmt.Errorf("no certificate in %s", *certFile)
	}
	res, err := runClient(context.Background(), clientConfig{server: *server, port: *port, mix: mix, duration: *duration, from: *from, stagger: *stagger, handshakeTimeout: 10 * time.Second, roots: roots})
	if err != nil {
		return err
	}
	failed := 0
	for _, f := range res.Flows {
		if f.Error != "" {
			failed++
		}
	}
	// A flow that failed competed for only part of the window, so the
	// shares don't describe the path: report them, but fail the run.
	var runErr error
	if failed > 0 {
		runErr = fmt.Errorf("%d of %d flows failed; the run isn't a valid measurement", failed, len(res.Flows))
	}
	if *asJSON {
		if err := json.NewEncoder(os.Stdout).Encode(res); err != nil {
			return err
		}
		return runErr
	}
	fmt.Printf("%-4s %-15s %7s %9s %7s %8s %9s\n", "flow", "sender", "start", "Mbit/s", "share", "minRTT*", "meanSRTT")
	for _, f := range res.Flows {
		fmt.Printf("%-4d %-15s %6.0fs %9.1f %6.0f%% %8s %9s", f.Flow, f.Sender, f.StartSeconds, f.Mbps, f.SharePercent, msOrDash(f.MinRTTms), msOrDash(f.MeanSRTTms))
		if f.Error != "" {
			fmt.Printf("  failed: %s", f.Error)
		}
		fmt.Println()
	}
	fmt.Printf("total %.1f Mbit/s over %.0fs (*minRTT is the connection's lifetime minimum)\n", res.TotalMbps, res.WindowSeconds)
	return runErr
}

// parseFlows turns "photon-bbr:2,quic-go:1" into one sender per flow.
func parseFlows(spec string) ([]string, error) {
	var mix []string
	for _, part := range strings.Split(spec, ",") {
		name, count, ok := strings.Cut(strings.TrimSpace(part), ":")
		n := 1
		if ok {
			var err error
			if n, err = strconv.Atoi(count); err != nil || n < 1 {
				return nil, fmt.Errorf("bad flow count in %q", part)
			}
		}
		if senderPort(name) < 0 {
			return nil, fmt.Errorf("unknown sender %q", name)
		}
		for range n {
			mix = append(mix, name)
		}
	}
	return mix, nil
}

func senderPort(name string) int {
	for i, s := range senders {
		if s == name {
			return i
		}
	}
	return -1
}

func runClient(ctx context.Context, cfg clientConfig) (runResult, error) {
	if cfg.stagger < 0 || cfg.from < 0 {
		return runResult{}, errors.New("stagger and measurement start must not be negative")
	}
	if cfg.duration-cfg.from <= time.Second {
		return runResult{}, errors.New("the measurement window must be over a second, to sample RTT")
	}
	if last := time.Duration(len(cfg.mix)-1) * cfg.stagger; last >= cfg.from {
		return runResult{}, fmt.Errorf("the last flow starts at %v, not before the measurement window at %v", last, cfg.from)
	}
	tlsConfig := &tls.Config{ServerName: "pathbench", RootCAs: cfg.roots, NextProtos: []string{alpn}}
	quicConfig := &upstream.Config{MaxIdleTimeout: 30 * time.Second, HandshakeIdleTimeout: cfg.handshakeTimeout, InitialStreamReceiveWindow: streamWindow, MaxStreamReceiveWindow: streamWindow, InitialConnectionReceiveWindow: connectionWindow, MaxConnectionReceiveWindow: connectionWindow}
	ctx, cancel := context.WithCancel(ctx)
	type flow struct {
		received atomic.Uint64
		err      atomic.Pointer[error]
		conn     atomic.Pointer[upstream.Conn]
		srtt     []time.Duration
		minRTT   time.Duration
	}
	flows := make([]*flow, len(cfg.mix))
	start := time.Now()
	var wg sync.WaitGroup
	// However the run ends, end the flows: closing each connection fails its
	// read, and lets the server log the connection's statistics.
	defer func() {
		cancel()
		for _, f := range flows {
			if f == nil {
				continue
			}
			if c := f.conn.Load(); c != nil {
				c.CloseWithError(0, "")
			}
		}
		wg.Wait()
	}()
	for i, sender := range cfg.mix {
		f := &flow{}
		flows[i] = f
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail := func(err error) { f.err.Store(&err) }
			select {
			case <-time.After(time.Until(start.Add(time.Duration(i) * cfg.stagger))):
			case <-ctx.Done():
				return
			}
			addr := net.JoinHostPort(cfg.server, strconv.Itoa(cfg.port+senderPort(sender)))
			dialCtx, stop := context.WithTimeout(ctx, cfg.handshakeTimeout)
			c, err := upstream.DialAddr(dialCtx, addr, tlsConfig, quicConfig)
			stop()
			if err != nil {
				fail(fmt.Errorf("dial %s: %w", addr, err))
				return
			}
			f.conn.Store(c)
			defer c.CloseWithError(0, "")
			s, err := c.OpenStreamSync(ctx)
			if err != nil {
				fail(err)
				return
			}
			if _, err := s.Write([]byte("get")); err != nil {
				fail(err)
				return
			}
			s.Close()
			buf := make([]byte, 64<<10)
			for {
				n, err := s.Read(buf)
				f.received.Add(uint64(n))
				if err != nil {
					if ctx.Err() == nil {
						fail(err)
					}
					return
				}
			}
		}()
	}
	// Sample each flow's RTT once a second through the window, and its bytes
	// at the window's edges.
	sleepUntil := func(t time.Time) bool {
		select {
		case <-time.After(time.Until(t)):
			return true
		case <-ctx.Done():
			return false
		}
	}
	if !sleepUntil(start.Add(cfg.from)) {
		return runResult{}, ctx.Err()
	}
	t0 := time.Now()
	base := make([]uint64, len(flows))
	present := make([]bool, len(flows)) // receiving data when the window opened
	for i, f := range flows {
		base[i] = f.received.Load()
		present[i] = base[i] > 0
	}
	for next := cfg.from + time.Second; next < cfg.duration; next += time.Second {
		if !sleepUntil(start.Add(next)) {
			return runResult{}, ctx.Err()
		}
		for _, f := range flows {
			// A failed flow's frozen statistics aren't samples of the window.
			if c := f.conn.Load(); c != nil && f.err.Load() == nil {
				st := c.ConnectionStats()
				f.srtt = append(f.srtt, st.SmoothedRTT)
				f.minRTT = st.MinRTT
			}
		}
	}
	if !sleepUntil(start.Add(cfg.duration)) {
		return runResult{}, ctx.Err()
	}
	// Each flow's bytes and fate as the window closes: a flow that fails
	// after it, as the run tears down, completed the window.
	window := time.Since(t0).Seconds()
	ends := make([]uint64, len(flows))
	errs := make([]*error, len(flows))
	for i, f := range flows {
		ends[i], errs[i] = f.received.Load(), f.err.Load()
	}
	samples := make([]flowSample, len(flows))
	for i, f := range flows {
		samples[i] = flowSample{sender: cfg.mix[i], start: time.Duration(i) * cfg.stagger, bytes: ends[i] - base[i], minRTT: f.minRTT, meanSRTT: mean(f.srtt)}
		switch {
		case errs[i] != nil:
			samples[i].err = (*errs[i]).Error()
		case !present[i]:
			samples[i].err = "no data received before the measurement window opened"
		}
	}
	res := summarize(cfg.server, window, samples)
	return res, nil
}

// flowSample is one flow's measurement over the window, before summary.
type flowSample struct {
	sender           string
	start            time.Duration
	bytes            uint64
	minRTT, meanSRTT time.Duration
	err              string
}

// summarize turns the window's samples into results. Every flow, failed or
// not, counts toward the total and the shares: a failed flow still used the
// path while it ran, and leaving it out would hand its share to the others.
func summarize(server string, window float64, samples []flowSample) runResult {
	res := runResult{Server: server, WindowSeconds: window}
	var total float64
	for i, f := range samples {
		mbps := float64(f.bytes) * 8 / window / 1e6
		res.Flows = append(res.Flows, flowResult{Flow: i + 1, Sender: f.sender, StartSeconds: f.start.Seconds(), Mbps: mbps, MinRTTms: ms(f.minRTT), MeanSRTTms: ms(f.meanSRTT), Error: f.err})
		total += mbps
	}
	res.TotalMbps = total
	for i := range res.Flows {
		if total > 0 {
			res.Flows[i].SharePercent = 100 * res.Flows[i].Mbps / total
		}
	}
	return res
}

func mean(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return sum / time.Duration(len(ds))
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// msOrDash prints a millisecond value, or "-" when there was no sample.
func msOrDash(v float64) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%.0fms", v)
}
