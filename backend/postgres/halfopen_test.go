package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/aetomala/worklease"
	wlpostgres "github.com/aetomala/worklease/backend/postgres"
)

// The halfOpenProxy type is an in-process TCP proxy between lib/pq and
// PostgreSQL. While black-holed, it forwards no bytes in either direction,
// keeps every connection open, and accepts but never serves new connections,
// so lib/pq's cancel request stalls too.
type halfOpenProxy struct {
	ln       net.Listener
	target   string
	mu       sync.Mutex
	conns    []net.Conn
	blackout bool
	held     atomic.Int64
}

// The startHalfOpenProxy function starts a proxy on a loopback port that
// forwards to target, a host:port address.
func startHalfOpenProxy(target string) (*halfOpenProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &halfOpenProxy{ln: ln, target: target}
	go p.serve()
	return p, nil
}

// The addr method returns the host:port the proxy listens on.
func (p *halfOpenProxy) addr() string { return p.ln.Addr().String() }

// The blackhole method stops all forwarding; connections stay open.
func (p *halfOpenProxy) blackhole() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blackout = true
}

// The heldBytes method returns how many bytes arrived while black-holed.
func (p *halfOpenProxy) heldBytes() int64 { return p.held.Load() }

// The reset method closes every connection on both sides, as TCP keepalive
// eventually does, and clears the black-hole for later connections.
func (p *halfOpenProxy) reset() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.blackout = false
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// The close method stops the listener and closes every connection.
func (p *halfOpenProxy) close() {
	_ = p.ln.Close()
	p.reset()
}

func (p *halfOpenProxy) holed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blackout
}

func (p *halfOpenProxy) track(c net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conns = append(p.conns, c)
}

// The serve method accepts connections. While black-holed, a new connection is
// kept but never dialed to the target, so it receives no reply.
func (p *halfOpenProxy) serve() {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.track(client)
		if p.holed() {
			go p.pipe(client, nil)
			continue
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.track(upstream)
		go p.pipe(client, upstream)
		go p.pipe(upstream, client)
	}
}

// The pipe method copies src to dst until either side ends, then closes both.
// While black-holed, it counts the bytes into held and does not forward them.
// A nil dst means src was accepted while black-holed and is never served.
func (p *halfOpenProxy) pipe(src, dst net.Conn) {
	defer func() {
		_ = src.Close()
		if dst != nil {
			_ = dst.Close()
		}
	}()
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if dst == nil || p.holed() {
				p.held.Add(int64(n))
			} else if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// The parseDSN function returns the connection parameters of a PostgreSQL URL
// or key/value DSN, including the URL's query parameters such as sslmode.
func parseDSN(dsn string) (map[string]string, error) {
	params := map[string]string{}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, err
		}
		params["host"] = u.Hostname()
		params["port"] = u.Port()
		if u.User != nil {
			params["user"] = u.User.Username()
			if pw, ok := u.User.Password(); ok {
				params["password"] = pw
			}
		}
		params["dbname"] = strings.TrimPrefix(u.Path, "/")
		for k, v := range u.Query() {
			params[k] = v[len(v)-1]
		}
	} else {
		for _, field := range strings.Fields(dsn) {
			k, v, ok := strings.Cut(field, "=")
			if !ok {
				return nil, fmt.Errorf("parseDSN: malformed field %q", field)
			}
			params[k] = strings.Trim(v, "'")
		}
	}
	if params["port"] == "" {
		params["port"] = "5432"
	}
	if params["host"] == "" {
		params["host"] = "localhost"
	}
	return params, nil
}

// The proxiedDSN function returns dsn in key/value form with host and port
// pointing at addr. Every other parameter of dsn, including a URL's query
// parameters such as sslmode, is kept: lib/pq defaults to sslmode=require.
func proxiedDSN(dsn, addr string) (string, error) {
	params, err := parseDSN(dsn)
	if err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	params["host"], params["port"] = host, port
	keys := make([]string, 0, len(params))
	for k := range params {
		if params[k] != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	fields := make([]string, 0, len(keys))
	for _, k := range keys {
		v := strings.ReplaceAll(strings.ReplaceAll(params[k], `\`, `\\`), `'`, `\'`)
		fields = append(fields, fmt.Sprintf("%s='%s'", k, v))
	}
	return strings.Join(fields, " "), nil
}

// The measureClockOffset function returns the server-minus-client clock
// offset and the round trip of the fastest of five SELECT clock_timestamp()
// samples.
func measureClockOffset(ctx context.Context, db *sql.DB) (offset, rtt time.Duration, err error) {
	rtt = time.Hour
	for range 5 {
		var server time.Time
		t0 := time.Now()
		if err = db.QueryRowContext(ctx, "SELECT clock_timestamp()").Scan(&server); err != nil {
			return 0, 0, err
		}
		t1 := time.Now()
		if d := t1.Sub(t0); d < rtt {
			rtt = d
			offset = server.Sub(t0.Add(d / 2))
		}
	}
	return offset, rtt, nil
}

// The stopWithin function runs stop in a goroutine and reports whether it
// returned within limit.
func stopWithin(stop func(), limit time.Duration) bool {
	done := make(chan struct{})
	go func() {
		stop()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(limit):
		return false
	}
}

// The renewWatcher type is a LeaseObserver whose renewed channel closes after
// the first successful Renew. Its other callbacks do nothing.
type renewWatcher struct {
	once    sync.Once
	renewed chan struct{}
}

var _ worklease.LeaseObserver = (*renewWatcher)(nil)

func (w *renewWatcher) OnAcquire(context.Context, worklease.AcquireEvent)       {}
func (w *renewWatcher) OnCheckpoint(context.Context, worklease.CheckpointEvent) {}
func (w *renewWatcher) OnRelease(context.Context, worklease.ReleaseEvent)       {}
func (w *renewWatcher) OnReadCheckpoint(context.Context, worklease.ReadCheckpointEvent) {
}
func (w *renewWatcher) OnForget(context.Context, worklease.ForgetEvent) {}
func (w *renewWatcher) OnFenced(context.Context, worklease.FencedEvent) {}
func (w *renewWatcher) OnRenew(_ context.Context, e worklease.RenewEvent) {
	if e.Err == nil {
		w.once.Do(func() { close(w.renewed) })
	}
}

var _ = Describe("half-open connection (#115)", func() {
	var (
		ctx     context.Context
		proxy   *halfOpenProxy
		lease   worklease.Lease
		watcher *renewWatcher
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		DeferCleanup(cancel)

		dsn := os.Getenv("WORKLEASE_TEST_POSTGRES_DSN")
		params, err := parseDSN(dsn)
		Expect(err).NotTo(HaveOccurred())
		proxy, err = startHalfOpenProxy(net.JoinHostPort(params["host"], params["port"]))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(proxy.close)

		pdsn, err := proxiedDSN(dsn, proxy.addr())
		Expect(err).NotTo(HaveOccurred())
		pdb, err := sql.Open("postgres", pdsn)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(pdb.Close)

		b, err := wlpostgres.New(pdb)
		Expect(err).NotTo(HaveOccurred())
		watcher = &renewWatcher{renewed: make(chan struct{})}
		lease, err = worklease.New(b, worklease.Config{TTL: 2 * time.Second, HolderID: "half-open", Observer: watcher})
		Expect(err).NotTo(HaveOccurred())
	})

	// stallRenewal acquires workID through the proxy, starts renewal every
	// 200ms, waits for the first successful renewal, black-holes the proxy, and
	// waits until the next Renew is in flight. closed receives the time renewCtx
	// was done. proxy.reset() is registered before any stopRenewal call, so a
	// regressed stopRenewal is released when the spec ends (C-E18).
	stallRenewal := func(workID string) (renewCtx context.Context, stop func(), closed <-chan time.Time) {
		token, err := lease.Acquire(ctx, workID)
		Expect(err).NotTo(HaveOccurred())
		renewCtx, stopRenewal := lease.StartRenewal(ctx, token, worklease.WithRenewalInterval(200*time.Millisecond))
		// A normal stopRenewal does not cancel renewCtx, so the watcher below also
		// ends once stop has returned.
		stopped := make(chan struct{})
		var stopOnce sync.Once
		stop = func() {
			stopRenewal()
			stopOnce.Do(func() { close(stopped) })
		}
		DeferCleanup(func() { stopWithin(stop, 5*time.Second) })
		DeferCleanup(proxy.reset)

		closedAt := make(chan time.Time, 1)
		go func() {
			select {
			case <-renewCtx.Done():
				closedAt <- time.Now()
			case <-stopped:
			}
		}()

		Eventually(watcher.renewed, 2*time.Second).Should(BeClosed())
		proxy.blackhole()
		Eventually(proxy.heldBytes, 2*time.Second).Should(BeNumerically(">", 0))
		return renewCtx, stop, closedAt
	}

	It("stopRenewal returns within 100ms while Renew is stalled on a black-holed connection", func() {
		_, stop, _ := stallRenewal("half-1")
		Expect(stopWithin(stop, 100*time.Millisecond)).To(BeTrue(), "stopRenewal waited on the stalled Renew")
	})

	It("renewCtx closes with ErrLeaseWindowExhausted no later than the stored expires_at, allowing for the measured clock offset", func() {
		renewCtx, stop, closed := stallRenewal("half-2")

		var closeAt time.Time
		Eventually(closed, 5*time.Second).Should(Receive(&closeAt))
		Expect(context.Cause(renewCtx)).To(MatchError(worklease.ErrLeaseWindowExhausted))

		// The stored expiry is in server time; convert it to client time.
		var expiresAt time.Time
		Expect(db.QueryRowContext(ctx, "SELECT expires_at FROM worklease_leases WHERE work_id = $1", "half-2").Scan(&expiresAt)).To(Succeed())
		offset, rtt, err := measureClockOffset(ctx, db)
		Expect(err).NotTo(HaveOccurred())
		expiry := expiresAt.Add(-offset)
		Expect(closeAt).To(BeTemporally("<=", expiry.Add(rtt/2+25*time.Millisecond)))
		Expect(closeAt).To(BeTemporally(">=", expiry.Add(-500*time.Millisecond)))

		Expect(stopWithin(stop, time.Second)).To(BeTrue())
	})

	It("after the proxy closes its connections, the abandoned Renew ends and the goroutine count returns to its baseline", func() {
		// Let goroutines left by earlier specs finish before taking the baseline.
		Eventually(func() bool {
			before := runtime.NumGoroutine()
			time.Sleep(200 * time.Millisecond)
			return runtime.NumGoroutine() == before
		}, 5*time.Second).Should(BeTrue())
		baseline := runtime.NumGoroutine()
		_, stop, _ := stallRenewal("half-3")
		Expect(stopWithin(stop, 100*time.Millisecond)).To(BeTrue())

		proxy.reset()
		Eventually(runtime.NumGoroutine, 5*time.Second).Should(BeNumerically("<=", baseline))
	})
})
