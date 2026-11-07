// Package proxy implements the request path:
//
//	lookup Redis index ─┬─ hit  → reverse proxy to the S3 public url (falls back to local on miss)
//	                    └─ miss → stream from local disk + async origin event
//
// Both dependencies are guarded: the index lookup has a short timeout and a circuit breaker,
// and S3 has its own breaker. Whenever either is unhealthy, requests are served from local.
package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"imagelib-s3-dynamic/internal/breaker"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/metrics"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

const TierHeader = "X-Storage-Tier"

type Handler struct {
	lookup       *indexLookup
	redisBreaker *breaker.Breaker
	s3Breaker    *breaker.Breaker

	local   *storage.Local
	index   *state.Index
	namer   storage.ObjectNamer
	pub     *events.Publisher
	touch   *toucher
	rp      *httputil.ReverseProxy
	nowFunc func() time.Time
}

type ctxKey struct{}

// upstreamMiss is returned from ModifyResponse to divert to the local fallback.
type upstreamMiss struct{ status int }

func (e *upstreamMiss) Error() string { return fmt.Sprintf("s3 upstream status %d", e.status) }

func New(cfg config.ProxyConfig, local *storage.Local, index *state.Index, namer storage.ObjectNamer, pub *events.Publisher) (*Handler, error) {
	redisBreaker := breaker.New("redis-index", cfg.RedisBreakerThreshold, cfg.RedisBreakerCooldown)
	lookup, err := newIndexLookup(index, redisBreaker, cfg.LookupTimeout, cfg.IndexCacheTTL, cfg.IndexCacheMaxKeys)
	if err != nil {
		return nil, err
	}
	h := &Handler{
		lookup:       lookup,
		redisBreaker: redisBreaker,
		s3Breaker:    breaker.New("s3-upstream", cfg.S3BreakerThreshold, cfg.S3BreakerCooldown),
		local:        local,
		index:        index,
		namer:        namer,
		pub:          pub,
		touch:        newToucher(index, cfg.TouchInterval),
		nowFunc:      time.Now,
	}
	for _, b := range []struct {
		name string
		br   *breaker.Breaker
	}{{"redis-index", redisBreaker}, {"s3-upstream", h.s3Breaker}} {
		g := metrics.BreakerState.WithLabelValues(b.name)
		b.br.OnStateChange(func(s breaker.State) { g.Set(float64(s)) })
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = cfg.UpstreamTimeout
	transport.MaxIdleConnsPerHost = 256
	transport.DialContext = (&net.Dialer{Timeout: min(cfg.UpstreamTimeout, 5*time.Second), KeepAlive: 30 * time.Second}).DialContext

	h.rp = &httputil.ReverseProxy{
		Transport:      transport,
		Rewrite:        h.rewrite,
		ModifyResponse: h.modifyResponse,
		ErrorHandler:   h.upstreamError,
		FlushInterval:  -1, // stream bytes to the client as they arrive
	}
	return h, nil
}

// Close stops background work (LRU touches and event publishing), bounded by ctx.
func (h *Handler) Close(ctx context.Context) {
	h.touch.close(ctx)
	h.pub.Close(ctx)
	h.lookup.close()
}

// Collector exports the index lookup and event publisher counters for /metrics.
func (h *Handler) Collector() prometheus.Collector {
	return collectors{h.lookup.collector(), h.pub.Collector()}
}

type collectors []prometheus.Collector

func (c collectors) Describe(ch chan<- *prometheus.Desc) {
	for _, x := range c {
		x.Describe(ch)
	}
}

func (c collectors) Collect(ch chan<- prometheus.Metric) {
	for _, x := range c {
		x.Collect(ch)
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mw := &meteredWriter{ResponseWriter: w}
	// Deferred: ReverseProxy aborts a broken stream (client gone, upstream cut) by panicking
	// with http.ErrAbortHandler; those partial responses must still be counted.
	defer h.observe(mw, r, time.Now())
	h.serve(mw, r)
}

func (h *Handler) observe(mw *meteredWriter, r *http.Request, start time.Time) {
	code := mw.code
	if code == 0 {
		code = http.StatusOK // net/http's implicit status for an empty response
		if r.Context().Err() != nil {
			code = 499 // client went away before anything was written
		}
	}
	tier := metrics.TierLocal
	if mw.Header().Get(TierHeader) == metrics.TierS3 {
		tier = metrics.TierS3
	}
	metrics.ObserveRequest(tier, code, mw.bytes, time.Since(start))
}

func (h *Handler) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key, err := storage.NormalizeKey(r.URL.Path)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if !h.lookup.inS3(r.Context(), key) {
		h.serveLocal(w, r, key, originRead)
		return
	}
	h.touch.add(key)
	if !h.s3Breaker.Allow() {
		// Already migrated; S3 is just unavailable. Not an origin read: don't feed heat.
		h.serveLocal(w, r, key, s3Fallback)
		return
	}
	h.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, key)))
}

// localMode says why a request is served from local disk, which decides what to report.
type localMode int

const (
	originRead    localMode = iota // not in S3: a genuine origin read → heat event
	s3Fallback                     // in S3 but S3 is failing (5xx/timeout/breaker) → no event
	staleFallback                  // index says S3 but S3 doesn't have it → stale event
)

// Health reports circuit breaker states and index cache counters for /healthz.
func (h *Handler) Health() map[string]string {
	out := h.lookup.stats()
	out["redis_breaker"] = h.redisBreaker.State().String()
	out["s3_breaker"] = h.s3Breaker.State().String()
	return out
}

func (h *Handler) rewrite(pr *httputil.ProxyRequest) {
	key := pr.In.Context().Value(ctxKey{}).(string)
	target, err := url.Parse(h.namer.PublicURL(key))
	if err != nil {
		// Unreachable for well-formed config; make the request fail and fall back.
		target = &url.URL{Scheme: "http", Host: "invalid.invalid"}
	}
	pr.Out.URL = target
	pr.Out.Host = target.Host
	// Don't leak client credentials to the bucket.
	pr.Out.Header.Del("Cookie")
	pr.Out.Header.Del("Authorization")
}

func (h *Handler) modifyResponse(resp *http.Response) error {
	// 404/403/… are per-object (stale index) problems, not an S3 outage.
	if resp.StatusCode >= 500 {
		h.s3Breaker.Failure()
		metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamServerError).Inc()
	} else {
		h.s3Breaker.Success()
	}
	switch {
	case resp.StatusCode == http.StatusNotFound,
		resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusUnauthorized,
		resp.StatusCode == http.StatusGone,
		resp.StatusCode >= 500:
		if resp.StatusCode < 500 {
			metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamObjectMissing).Inc()
		}
		resp.Body.Close()
		return &upstreamMiss{status: resp.StatusCode}
	}
	resp.Header.Del("Set-Cookie")
	// Caching is not managed by the S3 tier: drop any policy coming from object metadata or
	// bucket defaults so responses carry the same cache headers whichever tier served them.
	resp.Header.Del("Cache-Control")
	resp.Header.Del("Expires")
	resp.Header.Set(TierHeader, "s3")
	return nil
}

func (h *Handler) upstreamError(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		return // client went away
	}
	key := r.Context().Value(ctxKey{}).(string)
	var miss *upstreamMiss
	if !errors.As(err, &miss) {
		h.s3Breaker.Failure() // connection error / timeout (status errors were counted already)
		metrics.S3UpstreamErrors.WithLabelValues(metrics.UpstreamTransport).Inc()
	}
	stale := miss != nil && miss.status < 500
	mode := s3Fallback
	if stale {
		// Stop sending this key to S3 from this process until the scheduler has fixed the index.
		h.lookup.remember(key, false)
		mode = staleFallback
	}
	slog.Warn("s3 upstream failed, falling back to local", "key", key, "err", err, "stale", stale)
	h.serveLocal(w, r, key, mode)
}

func (h *Handler) serveLocal(w http.ResponseWriter, r *http.Request, key string, mode localMode) {
	localServes[mode].Inc()
	stale := mode == staleFallback
	f, info, err := h.local.Open(key)
	if err != nil {
		if stale {
			h.pub.Submit(events.Event{Key: key, At: h.nowFunc(), Stale: true})
		}
		if errors.Is(err, fs.ErrNotExist) {
			http.NotFound(w, r)
			return
		}
		slog.Error("open local object", "key", key, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	// Fire the event before streaming so slow clients don't delay heat tracking.
	// HEAD requests are not counted as reads; S3 fallbacks are not origin reads.
	if stale || (mode == originRead && r.Method == http.MethodGet) {
		h.pub.Submit(events.Event{Key: key, Size: info.Size(), At: h.nowFunc(), Stale: stale})
	}

	hdr := w.Header()
	hdr.Set(TierHeader, "local")
	if hdr.Get("Etag") == "" {
		hdr.Set("Etag", `W/"`+strconv.FormatInt(info.Size(), 36)+"-"+strconv.FormatInt(info.ModTime().UnixNano(), 36)+`"`)
	}
	// ServeContent streams straight from disk and handles Range/HEAD/conditional requests.
	http.ServeContent(w, r, info.Name(), info.ModTime(), f)
}

// localServes is indexed by localMode.
var localServes = [...]prometheus.Counter{
	originRead:    metrics.LocalServes.WithLabelValues(metrics.LocalOrigin),
	s3Fallback:    metrics.LocalServes.WithLabelValues(metrics.LocalS3Fallback),
	staleFallback: metrics.LocalServes.WithLabelValues(metrics.LocalStaleFallback),
}

// meteredWriter records the status code and body bytes of a response. It forwards
// io.ReaderFrom (sendfile) and exposes Unwrap for http.ResponseController (flushing).
type meteredWriter struct {
	http.ResponseWriter
	code  int
	bytes int64
}

func (w *meteredWriter) WriteHeader(code int) {
	if w.code == 0 && code >= 200 { // 1xx are informational; the final status follows
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *meteredWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

func (w *meteredWriter) ReadFrom(src io.Reader) (n int64, err error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	if rf, ok := w.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
	} else {
		n, err = io.Copy(w.ResponseWriter, src)
	}
	w.bytes += n
	return n, err
}

func (w *meteredWriter) Flush() {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *meteredWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
