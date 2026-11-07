package metrics

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "imagelib"

// Tier label values of request metrics.
const (
	TierLocal = "local"
	TierS3    = "s3"
)

// --- request path ---

var (
	Requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "requests_total",
		Help: "Image requests by serving tier and HTTP status code.",
	}, []string{"tier", "code"})
	RequestBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "request_bytes_total",
		Help: "Response body bytes written to clients by serving tier.",
	}, []string{"tier"})
	RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "request_duration_seconds",
		Help:    "Image request duration including the client download, by serving tier.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
	}, []string{"tier"})
)

// tierMetrics caches resolved children so the hot path skips label hashing.
type tierMetrics struct {
	bytes    prometheus.Counter
	duration prometheus.Observer
	codes    [600]atomic.Pointer[prometheus.Counter] // indexed by status code; 0 = out of range
}

var tiers = [2]*tierMetrics{newTierMetrics(TierLocal), newTierMetrics(TierS3)}

func newTierMetrics(tier string) *tierMetrics {
	return &tierMetrics{
		bytes:    RequestBytes.WithLabelValues(tier),
		duration: RequestDuration.WithLabelValues(tier),
	}
}

func (t *tierMetrics) code(tier string, code int) prometheus.Counter {
	if code < 100 || code >= len(t.codes) {
		code = 0
	}
	if c := t.codes[code].Load(); c != nil {
		return *c
	}
	label := "other"
	if code != 0 {
		label = strconv.Itoa(code)
	}
	c := Requests.WithLabelValues(tier, label)
	t.codes[code].Store(&c)
	return c
}

// ObserveRequest records one finished image request.
func ObserveRequest(tier string, code int, bytes int64, d time.Duration) {
	i := 0
	if tier == TierS3 {
		i = 1
	}
	t := tiers[i]
	t.code(tier, code).Inc()
	if bytes > 0 {
		t.bytes.Add(float64(bytes))
	}
	t.duration.Observe(d.Seconds())
}

// --- S3 upstream health & local serving ---

// Upstream error kinds.
const (
	UpstreamObjectMissing = "object_missing" // 401/403/404/410: stale index entry
	UpstreamServerError   = "server_error"   // 5xx
	UpstreamTransport     = "transport"      // connect error / timeout
)

// Local serve reasons.
const (
	LocalOrigin        = "origin"
	LocalS3Fallback    = "s3_fallback"
	LocalStaleFallback = "stale_fallback"
)

var (
	S3UpstreamErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "s3_upstream_errors_total",
		Help: "Failed S3 upstream requests by kind (object_missing|server_error|transport).",
	}, []string{"kind"})
	LocalServes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "local_serves_total",
		Help: "Requests served from local disk by reason (origin|s3_fallback|stale_fallback).",
	}, []string{"reason"})
	BreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "breaker_state",
		Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open.",
	}, []string{"name"})
)

// --- scheduler (leader only) ---

// Migration results.
const (
	MigrationSuccess  = "success"
	MigrationFailure  = "failure"
	MigrationDeferred = "deferred" // no evictable space
	MigrationSkipped  = "skipped"  // already in S3, gone, not eligible, index not ready
)

// Reconcile fix kinds.
const (
	FixRestored      = "restored"
	FixDropped       = "dropped"
	FixFinished      = "finished"
	FixOrphanDeleted = "orphan_deleted"
)

var (
	EventsLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "events_lag",
		Help: "Entries in the event stream not yet delivered to the migrator group (leader only).",
	})
	Leader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "leader",
		Help: "1 while this process runs the scheduler as leader.",
	})
	Migrations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "migrations_total",
		Help: "Migration attempts by result (success|failure|deferred|skipped).",
	}, []string{"result"})
	MigrationBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "migrations_bytes_total",
		Help: "Bytes uploaded to S3 by successful migrations.",
	})
	MigrationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns, Name: "migration_duration_seconds",
		Help:    "Upload + commit duration of successful migrations.",
		Buckets: prometheus.ExponentialBuckets(0.025, 2, 13), // 25ms … ~102s
	})
	Evictions = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "evictions_total",
		Help: "Objects evicted from S3 (de-indexed, delete scheduled).",
	})
	EvictionBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "evictions_bytes_total",
		Help: "Bytes freed by evictions.",
	})
	S3Deletes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "s3_deletes_total",
		Help: "Trash S3 deletes by result (success|failure).",
	}, []string{"result"})
	S3IndexBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "s3_index_bytes",
		Help: "Total bytes of objects indexed as S3-resident.",
	})
	S3CapacityBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "s3_capacity_bytes",
		Help: "Configured S3 capacity (S3_CAPACITY).",
	})
	MigrateQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "migrate_queue_depth",
		Help: "Migration jobs waiting for a worker.",
	})
	BusyKeys = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "busy_keys",
		Help: "Keys locked by in-flight scheduler work.",
	})
	BackoffKeys = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "backoff_keys",
		Help: "Keys whose migration is backing off for lack of evictable space.",
	})
	HeatGCRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "heat_gc_removed_total",
		Help: "Decayed heat entries removed by heat GC.",
	})
	StaleIndexRemovals = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "stale_index_removals_total",
		Help: "Index entries removed because proxies found the object missing in S3.",
	})
	ReconcileFixes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "reconcile_fixes_total",
		Help: "Drift fixed by reconcile, by kind (restored|dropped|finished|orphan_deleted).",
	}, []string{"kind"})
)

func init() {
	// Pre-create label combinations so series exist (as 0) before the first event.
	for _, k := range []string{UpstreamObjectMissing, UpstreamServerError, UpstreamTransport} {
		S3UpstreamErrors.WithLabelValues(k)
	}
	for _, r := range []string{LocalOrigin, LocalS3Fallback, LocalStaleFallback} {
		LocalServes.WithLabelValues(r)
	}
	for _, r := range []string{MigrationSuccess, MigrationFailure, MigrationDeferred, MigrationSkipped} {
		Migrations.WithLabelValues(r)
	}
	for _, r := range []string{"success", "failure"} {
		S3Deletes.WithLabelValues(r)
	}
	for _, k := range []string{FixRestored, FixDropped, FixFinished, FixOrphanDeleted} {
		ReconcileFixes.WithLabelValues(k)
	}
}

// NewRegistry returns a registry with the Go runtime and process collectors, all package
// metrics and the given per-instance collectors.
func NewRegistry(extra ...prometheus.Collector) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		Requests, RequestBytes, RequestDuration,
		S3UpstreamErrors, LocalServes, BreakerState,
		EventsLag, Leader, Migrations, MigrationBytes, MigrationDuration,
		Evictions, EvictionBytes, S3Deletes, S3IndexBytes, S3CapacityBytes,
		MigrateQueueDepth, BusyKeys, BackoffKeys,
		HeatGCRemoved, StaleIndexRemovals, ReconcileFixes,
	)
	for _, c := range extra {
		if c != nil {
			reg.MustRegister(c)
		}
	}
	return reg
}

// Handler serves reg in the Prometheus text format (scraped by vmagent).
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// Func is a value read at scrape time.
type Func struct {
	Name, Help string
	Gauge      bool // counter otherwise
	Value      func() float64
}

// FuncCollector exports values read at scrape time (e.g. existing atomic counters).
func FuncCollector(fs ...Func) prometheus.Collector {
	cs := make(funcCollector, len(fs))
	for i, f := range fs {
		opts := prometheus.Opts{Namespace: ns, Name: f.Name, Help: f.Help}
		if f.Gauge {
			cs[i] = prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), f.Value)
		} else {
			cs[i] = prometheus.NewCounterFunc(prometheus.CounterOpts(opts), f.Value)
		}
	}
	return cs
}

type funcCollector []prometheus.Collector

func (c funcCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, x := range c {
		x.Describe(ch)
	}
}

func (c funcCollector) Collect(ch chan<- prometheus.Metric) {
	for _, x := range c {
		x.Collect(ch)
	}
}
