package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Role selects which components a process runs.
type Role string

const (
	RoleAll       Role = "all"       // proxy + scheduler (scheduler only runs while holding leadership)
	RoleProxy     Role = "proxy"     // reverse proxy only
	RoleScheduler Role = "scheduler" // migration scheduler only (plus /healthz)
)

type Config struct {
	Addr       string
	Role       Role
	InstanceID string
	Database   string
	DBPush     DBPushConfig
	Redis      RedisConfig
	Storage    StorageConfig
	LocalPath  string
	Proxy      ProxyConfig
	Scheduler  SchedulerConfig
}

// DBPushConfig controls applying the embedded schema to PostgreSQL on startup.
type DBPushConfig struct {
	Enabled          bool
	AllowDestructive bool // allow drops / lossy type changes
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	Prefix   string        // key namespace, e.g. "imagelib:"
	Timeout  time.Duration // dial/read/write timeout of the client
}

type StorageConfig struct {
	Endpoint       string // e.g. https://s3.us-west-004.backblazeb2.com
	Region         string // e.g. us-west-004
	Bucket         string
	AccessKey      string
	SecretKey      string
	Prefix         string // Option
	PublicURL      string
	ForcePathStyle bool
}

type ProxyConfig struct {
	LookupTimeout         time.Duration // per-request index lookup timeout
	RedisBreakerThreshold int           // consecutive lookup failures before serving local without asking Redis
	RedisBreakerCooldown  time.Duration
	S3BreakerThreshold    int // consecutive S3 failures (5xx/timeout/conn) before bypassing S3
	S3BreakerCooldown     time.Duration
	EventBuffer           int           // in-memory buffer of pending origin events
	StreamMaxLen          int64         // approximate MAXLEN of the redis event stream
	TouchInterval         time.Duration // how often S3-hit LRU touches are flushed
	UpstreamTimeout       time.Duration // response header timeout for the S3 public url
}

type SchedulerConfig struct {
	CapacityBytes      int64         // max total bytes kept in S3
	Threshold          float64       // decayed hit score required to migrate
	HalfLife           time.Duration // heat decay half-life
	Workers            int           // concurrent uploads
	MinSize            int64         // objects smaller than this stay local
	MaxSize            int64         // objects larger than this stay local
	EvictInterval      time.Duration // periodic capacity enforcement
	EvictMinIdle       time.Duration // admitting a new object may only evict objects idle at least this long
	RetryBackoff       time.Duration // wait before retrying a migration rejected for lack of space
	DeleteDelay        time.Duration // grace period between de-indexing and deleting from S3
	StatsFlushInterval time.Duration // PG hits/last_access flush interval
	LeaderTTL          time.Duration // leader lock TTL
	IndexCheckInterval time.Duration // how often the leader checks the Redis index still exists
	ReconcileInterval  time.Duration // PG ↔ Redis ↔ S3 reconciliation interval (0 disables)
	ReconcileOrphans   string        // off | log | delete: S3 objects under the prefix unknown to the index
	OrphanMinAge       time.Duration // never treat objects younger than this as orphans
}

func Load() (Config, error) {
	_ = godotenv.Load()

	host, _ := os.Hostname()
	p := &parser{}

	cfg := Config{
		Addr:       getEnv("ADDR", ":8080"),
		Role:       Role(strings.ToLower(getEnv("ROLE", string(RoleAll)))),
		InstanceID: getEnv("INSTANCE_ID", fmt.Sprintf("%s-%d", host, os.Getpid())),
		Database:   getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/imagelib?sslmode=disable"),
		LocalPath:  getEnv("LOCAL_PATH", "./data"),
		DBPush: DBPushConfig{
			Enabled:          p.bool("DB_AUTO_PUSH", true),
			AllowDestructive: p.bool("DB_PUSH_ALLOW_DESTRUCTIVE", false),
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       p.int("REDIS_DB", 0),
			Prefix:   getEnv("REDIS_PREFIX", "imagelib:"),
			Timeout:  p.duration("REDIS_TIMEOUT", time.Second),
		},
		Storage: StorageConfig{
			Endpoint:       getEnv("S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com"),
			Region:         getEnv("S3_REGION", "us-west-004"),
			Bucket:         os.Getenv("S3_BUCKET"),
			AccessKey:      os.Getenv("B2_APPLICATION_KEY_ID"),
			SecretKey:      os.Getenv("B2_APPLICATION_KEY"),
			Prefix:         strings.Trim(os.Getenv("S3_PREFIX"), "/"),
			PublicURL:      strings.TrimRight(os.Getenv("S3_PUBLIC_URL"), "/"),
			ForcePathStyle: p.bool("S3_FORCE_PATH_STYLE", false),
		},
		Proxy: ProxyConfig{
			LookupTimeout:         p.duration("REDIS_LOOKUP_TIMEOUT", 100*time.Millisecond),
			RedisBreakerThreshold: p.int("REDIS_BREAKER_THRESHOLD", 5),
			RedisBreakerCooldown:  p.duration("REDIS_BREAKER_COOLDOWN", 5*time.Second),
			S3BreakerThreshold:    p.int("S3_BREAKER_THRESHOLD", 5),
			S3BreakerCooldown:     p.duration("S3_BREAKER_COOLDOWN", 30*time.Second),
			EventBuffer:           p.int("EVENT_BUFFER", 8192),
			StreamMaxLen:          int64(p.int("EVENT_STREAM_MAXLEN", 100000)),
			TouchInterval:         p.duration("LRU_TOUCH_INTERVAL", time.Second),
			UpstreamTimeout:       p.duration("S3_UPSTREAM_TIMEOUT", 10*time.Second),
		},
		Scheduler: SchedulerConfig{
			CapacityBytes:      p.size("S3_CAPACITY", "10GiB"),
			Threshold:          p.float("MIGRATE_THRESHOLD", 3),
			HalfLife:           p.duration("MIGRATE_HALF_LIFE", time.Hour),
			Workers:            p.int("MIGRATE_WORKERS", 4),
			MinSize:            p.size("MIGRATE_MIN_SIZE", "0"),
			MaxSize:            p.size("MIGRATE_MAX_SIZE", "512MiB"),
			EvictInterval:      p.duration("EVICT_INTERVAL", time.Minute),
			EvictMinIdle:       p.duration("EVICT_MIN_IDLE", 10*time.Minute),
			RetryBackoff:       p.duration("MIGRATE_RETRY_BACKOFF", time.Minute),
			DeleteDelay:        p.duration("EVICT_DELETE_DELAY", 30*time.Second),
			StatsFlushInterval: p.duration("STATS_FLUSH_INTERVAL", 10*time.Second),
			LeaderTTL:          p.duration("LEADER_TTL", 15*time.Second),
			IndexCheckInterval: p.duration("INDEX_CHECK_INTERVAL", 10*time.Second),
			ReconcileInterval:  p.duration("RECONCILE_INTERVAL", time.Hour),
			ReconcileOrphans:   strings.ToLower(getEnv("RECONCILE_ORPHANS", "log")),
			OrphanMinAge:       p.duration("RECONCILE_ORPHAN_MIN_AGE", time.Hour),
		},
	}
	if p.err != nil {
		return Config{}, p.err
	}
	return cfg, cfg.validate()
}

func (c Config) validate() error {
	switch c.Role {
	case RoleAll, RoleProxy, RoleScheduler:
	default:
		return fmt.Errorf("ROLE must be one of all|proxy|scheduler, got %q", c.Role)
	}
	if c.Role != RoleScheduler && c.Storage.PublicURL == "" {
		return fmt.Errorf("S3_PUBLIC_URL is required for the proxy")
	}
	if c.Role != RoleProxy {
		if c.Storage.Bucket == "" {
			return fmt.Errorf("S3_BUCKET is required for the scheduler")
		}
		if c.Scheduler.CapacityBytes <= 0 {
			return fmt.Errorf("S3_CAPACITY must be > 0")
		}
		if c.Scheduler.Workers <= 0 {
			return fmt.Errorf("MIGRATE_WORKERS must be > 0")
		}
		switch c.Scheduler.ReconcileOrphans {
		case "off", "log", "delete":
		default:
			return fmt.Errorf("RECONCILE_ORPHANS must be off|log|delete, got %q", c.Scheduler.ReconcileOrphans)
		}
		if c.Scheduler.ReconcileOrphans == "delete" && c.Storage.Prefix == "" {
			return fmt.Errorf("RECONCILE_ORPHANS=delete requires S3_PREFIX: the prefix must be owned exclusively by this service")
		}
		if c.Scheduler.IndexCheckInterval <= 0 {
			return fmt.Errorf("INDEX_CHECK_INTERVAL must be > 0")
		}
		if c.Scheduler.HalfLife <= 0 || c.Scheduler.LeaderTTL < time.Second {
			return fmt.Errorf("MIGRATE_HALF_LIFE must be > 0 and LEADER_TTL >= 1s")
		}
	}
	if c.Redis.Timeout <= 0 || c.Proxy.LookupTimeout <= 0 {
		return fmt.Errorf("REDIS_TIMEOUT and REDIS_LOOKUP_TIMEOUT must be > 0")
	}
	if c.Proxy.EventBuffer <= 0 || c.Proxy.TouchInterval <= 0 {
		return fmt.Errorf("EVENT_BUFFER and LRU_TOUCH_INTERVAL must be > 0")
	}
	return nil
}

// RunsProxy reports whether this process serves image traffic.
func (c Config) RunsProxy() bool { return c.Role == RoleAll || c.Role == RoleProxy }

// RunsScheduler reports whether this process takes part in leader election.
func (c Config) RunsScheduler() bool { return c.Role == RoleAll || c.Role == RoleScheduler }

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parser collects the first parse error so Load can report it once.
type parser struct{ err error }

func (p *parser) fail(key, v string, err error) {
	if p.err == nil {
		p.err = fmt.Errorf("invalid %s=%q: %w", key, v, err)
	}
}

func (p *parser) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(key, v, err)
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail(key, v, err)
	}
	return f
}

func (p *parser) bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(key, v, err)
	}
	return b
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, v, err)
	}
	return d
}

func (p *parser) size(key, def string) int64 {
	v := getEnv(key, def)
	n, err := ParseSize(v)
	if err != nil {
		p.fail(key, v, err)
	}
	return n
}

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"TIB", 1 << 40}, {"GIB", 1 << 30}, {"MIB", 1 << 20}, {"KIB", 1 << 10},
	{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
	{"T", 1 << 40}, {"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
	{"B", 1},
}

// ParseSize parses human sizes such as "512MiB", "10GB", "1.5G" or "1048576".
func ParseSize(s string) (int64, error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range sizeUnits {
		if strings.HasSuffix(t, u.suffix) {
			t = strings.TrimSpace(strings.TrimSuffix(t, u.suffix))
			mult = u.mult
			break
		}
	}
	f, err := strconv.ParseFloat(t, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("bad size")
	}
	return int64(f * float64(mult)), nil
}
