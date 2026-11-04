// Package proxy implements the request path:
//
//	lookup Redis index ─┬─ hit  → reverse proxy to the S3 public url (falls back to local on miss)
//	                    └─ miss → stream from local disk + async origin event
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

	"imagelib-s3-dynamic/internal/config"
	"imagelib-s3-dynamic/internal/events"
	"imagelib-s3-dynamic/internal/state"
	"imagelib-s3-dynamic/internal/storage"
)

const TierHeader = "X-Storage-Tier"

type Handler struct {
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
		local:   local,
		index:   index,
		namer:   namer,
		pub:     pub,
		touch:   newToucher(index, cfg.TouchInterval),
		nowFunc: time.Now,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = cfg.UpstreamTimeout
	transport.MaxIdleConnsPerHost = 256
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext

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

	inS3, err := h.index.Contains(r.Context(), key)
	if err != nil {
		// Redis trouble must not take the site down: local always has the object.
		slog.Warn("index lookup failed, serving local", "key", key, "err", err)
		inS3 = false
	}
	if inS3 {
		h.touch.add(key)
		h.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, key)))
		return
	}
	h.serveLocal(w, r, key, false)
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
	stale := errors.As(err, &miss) && miss.status < 500
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
