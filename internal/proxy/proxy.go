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
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"imagelib-s3-dynamic/internal/breaker"
	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

const TierHeader = "X-Storage-Tier"

type Handler struct {
	lookupTimeout time.Duration
	redisBreaker  *breaker.Breaker
	s3Breaker     *breaker.Breaker

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

func New(cfg config.ProxyConfig, local *storage.Local, index *state.Index, namer storage.ObjectNamer, pub *events.Publisher) *Handler {
	h := &Handler{
		lookupTimeout: cfg.LookupTimeout,
		redisBreaker:  breaker.New("redis-index", cfg.RedisBreakerThreshold, cfg.RedisBreakerCooldown),
		s3Breaker:     breaker.New("s3-upstream", cfg.S3BreakerThreshold, cfg.S3BreakerCooldown),
		local:         local,
		index:         index,
		namer:         namer,
		pub:           pub,
		touch:         newToucher(index, cfg.TouchInterval),
		nowFunc:       time.Now,
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
	return h
}

// Close stops background work (LRU touches and event publishing), bounded by ctx.
func (h *Handler) Close(ctx context.Context) {
	h.touch.close(ctx)
	h.pub.Close(ctx)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	if h.lookup(r, key) && h.s3Breaker.Allow() {
		h.touch.add(key)
		h.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, key)))
		return
	}
	h.serveLocal(w, r, key, false)
}

// lookup asks the index whether key lives in S3. Any Redis trouble (error, timeout, open
// breaker) answers false: local always has the object, so degrading is always correct.
func (h *Handler) lookup(r *http.Request, key string) bool {
	if !h.redisBreaker.Allow() {
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.lookupTimeout)
	defer cancel()
	inS3, err := h.index.Contains(ctx, key)
	switch {
	case err == nil:
		h.redisBreaker.Success()
		return inS3
	case r.Context().Err() != nil:
		return false // client went away; says nothing about Redis health
	default:
		h.redisBreaker.Failure()
		slog.Warn("index lookup failed, serving local", "key", key, "err", err)
		return false
	}
}

// Health reports dependency circuit breaker states for /healthz.
func (h *Handler) Health() map[string]string {
	return map[string]string{
		"redis_breaker": h.redisBreaker.State().String(),
		"s3_breaker":    h.s3Breaker.State().String(),
	}
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
	} else {
		h.s3Breaker.Success()
	}
	switch {
	case resp.StatusCode == http.StatusNotFound,
		resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusUnauthorized,
		resp.StatusCode == http.StatusGone,
		resp.StatusCode >= 500:
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
	}
	stale := miss != nil && miss.status < 500
	slog.Warn("s3 upstream failed, falling back to local", "key", key, "err", err, "stale", stale)
	h.serveLocal(w, r, key, stale)
}

func (h *Handler) serveLocal(w http.ResponseWriter, r *http.Request, key string, stale bool) {
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

	// Fire the origin event before streaming so slow clients don't delay heat tracking.
	// HEAD requests are not counted as reads.
	if r.Method == http.MethodGet || stale {
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
