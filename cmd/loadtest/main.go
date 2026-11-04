// Command loadtest generates a dataset and drives HTTP load against imagelib.
//
//	loadtest gen -dir /tmp/bench/data -n 1000                 # dataset + manifest.tsv
//	loadtest run -url http://127.0.0.1:8080 -manifest /tmp/bench/data/manifest.tsv \
//	    -c 64 -d 30s -dist zipf -redis localhost:56379 -out result.json
//
// Every response is checked for status and length against the manifest; a sample is also
// hashed. TTFB is measured with httptrace (first response byte).
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: loadtest gen|run [flags]")
	}
	switch os.Args[1] {
	case "gen":
		gen(os.Args[2:])
	case "run":
		run(os.Args[2:])
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}

// ---------------------------------------------------------------------------- gen

type object struct {
	Key  string
	Size int64
	Sum  string
}

func gen(args []string) {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	dir := fs.String("dir", "", "output directory")
	n := fs.Int("n", 1000, "number of objects")
	seed := fs.Uint64("seed", 42, "size distribution seed")
	_ = fs.Parse(args)
	if *dir == "" {
		log.Fatal("-dir is required")
	}
	r := mrand.New(mrand.NewPCG(*seed, *seed))
	mf, err := os.Create(filepath.Join(*dir, "manifest.tsv"))
	if err != nil {
		_ = os.MkdirAll(*dir, 0o755)
		mf, err = os.Create(filepath.Join(*dir, "manifest.tsv"))
		if err != nil {
			log.Fatal(err)
		}
	}
	defer mf.Close()
	var total int64
	for i := range *n {
		// Image-like sizes: 70% 8–100 KiB, 25% 100 KiB–1 MiB, 5% 1–4 MiB.
		var size int64
		switch p := r.Float64(); {
		case p < 0.70:
			size = 8<<10 + r.Int64N(92<<10)
		case p < 0.95:
			size = 100<<10 + r.Int64N(924<<10)
		default:
			size = 1<<20 + r.Int64N(3<<20)
		}
		key := fmt.Sprintf("img/%02d/%05d.jpg", i%100, i)
		buf := make([]byte, size)
		_, _ = rand.Read(buf)
		p := filepath.Join(*dir, key)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, buf, 0o644); err != nil {
			log.Fatal(err)
		}
		sum := sha256.Sum256(buf)
		fmt.Fprintf(mf, "%s\t%d\t%s\n", key, size, hex.EncodeToString(sum[:]))
		total += size
	}
	fmt.Printf("generated %d objects, %.1f MiB in %s\n", *n, float64(total)/(1<<20), *dir)
}

func loadManifest(path string, limit int) []object {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	var objs []object
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), "\t")
		if len(parts) != 3 {
			continue
		}
		size, _ := strconv.ParseInt(parts[1], 10, 64)
		objs = append(objs, object{Key: parts[0], Size: size, Sum: parts[2]})
		if limit > 0 && len(objs) == limit {
			break
		}
	}
	return objs
}

// ---------------------------------------------------------------------------- run

type sample struct {
	at    time.Duration // since start
	ttfb  time.Duration
	total time.Duration
	tier  string
	ok    bool
	bytes int64
}

type Percentiles struct {
	P50, P90, P99, P999, Max float64 // milliseconds
}

type TierStats struct {
	Requests int64
	TTFB     Percentiles
	Total    Percentiles
}

type Bucket struct {
	Second  int
	RPS     float64
	P50TTFB float64
	P99TTFB float64
	S3      int64
	Local   int64
	Errors  int64
	S3Bytes int64 `json:",omitempty"`
	MaxTTFB float64
}

type Result struct {
	Label        string
	Rate         string
	Concurrency  int
	Duration     float64
	Requests     int64
	Errors       int64
	Mismatches   int64
	HashChecked  int64
	RPS          float64
	MBps         float64
	TTFB         Percentiles
	Total        Percentiles
	Tiers        map[string]*TierStats
	RedisCmds    map[string]int64 `json:",omitempty"`
	RedisPerReq  float64          `json:",omitempty"`
	Timeline     []Bucket         `json:",omitempty"`
	MaxS3Bytes   int64            `json:",omitempty"`
	ErrorSamples []string         `json:",omitempty"`
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	base := fs.String("url", "http://127.0.0.1:8080", "imagelib base url")
	manifest := fs.String("manifest", "", "manifest.tsv from gen")
	keys := fs.Int("keys", 0, "use only the first N objects (0 = all)")
	conc := fs.Int("c", 64, "concurrent workers")
	dur := fs.Duration("d", 30*time.Second, "duration")
	dist := fs.String("dist", "zipf", "key distribution: zipf | uniform")
	zipfS := fs.Float64("zipf-s", 1.1, "zipf s parameter (>1, larger = more skewed)")
	verify := fs.Float64("verify", 0.02, "fraction of responses to sha256-verify")
	redisAddr := fs.String("redis", "", "redis addr for INFO commandstats delta (optional)")
	bytesKey := fs.String("bytes-key", "", "redis key holding S3 bytes, sampled each second (optional)")
	timeline := fs.Bool("timeline", false, "record per-second buckets")
	rate := fs.Float64("rate", 0, "fixed total request rate (open loop); 0 = closed loop, as fast as possible")
	label := fs.String("label", "", "result label")
	out := fs.String("out", "", "write JSON result here")
	_ = fs.Parse(args)

	objs := loadManifest(*manifest, *keys)
	if len(objs) == 0 {
		log.Fatal("empty manifest")
	}

	var rdb *redis.Client
	var before map[string]int64
	if *redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: *redisAddr})
		defer rdb.Close()
		before = commandStats(rdb)
	}

	tr := &http.Transport{MaxIdleConnsPerHost: *conc * 2, MaxIdleConns: *conc * 2, DisableCompression: true}
	client := &http.Client{Transport: tr, Timeout: 60 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	start := time.Now()

	// S3 bytes sampler.
	var s3BytesMu sync.Mutex
	s3Bytes := map[int]int64{}
	if rdb != nil && *bytesKey != "" {
		go func() {
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					n, _ := rdb.Get(context.Background(), *bytesKey).Int64()
					sec := int(time.Since(start) / time.Second)
					s3BytesMu.Lock()
					s3Bytes[sec] = max(s3Bytes[sec], n)
					s3BytesMu.Unlock()
				}
			}
		}()
	}

	var (
		mismatches, hashed atomic.Int64
		errMu              sync.Mutex
		errSamples         []string
		wg                 sync.WaitGroup
	)
	perWorker := make([][]sample, *conc)
	for w := range *conc {
		wg.Go(func() {
			r := mrand.New(mrand.NewPCG(uint64(w)+1, uint64(time.Now().UnixNano())))
			var z *mrand.Zipf
			if *dist == "zipf" {
				z = mrand.NewZipf(r, *zipfS, 1, uint64(len(objs)-1))
			}
			samples := make([]sample, 0, 4096)
			var pace *time.Ticker
			if *rate > 0 {
				pace = time.NewTicker(time.Duration(float64(*conc) / *rate * float64(time.Second)))
				defer pace.Stop()
			}
			for ctx.Err() == nil {
				if pace != nil {
					select {
					case <-pace.C:
					case <-ctx.Done():
						continue
					}
				}
				var o object
				if z != nil {
					o = objs[z.Uint64()]
				} else {
					o = objs[r.IntN(len(objs))]
				}
				s, err := fetch(ctx, client, *base, o, r.Float64() < *verify, &hashed)
				if ctx.Err() != nil && err != nil {
					break // cut off by the deadline, not a failure
				}
				s.at = time.Since(start)
				if err != nil {
					s.ok = false
					if strings.Contains(err.Error(), "mismatch") {
						mismatches.Add(1)
					}
					errMu.Lock()
					if len(errSamples) < 10 {
						errSamples = append(errSamples, err.Error())
					}
					errMu.Unlock()
				}
				samples = append(samples, s)
			}
			perWorker[w] = samples
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	var all []sample
	for _, s := range perWorker {
		all = append(all, s...)
	}
	res := summarize(all, elapsed, *timeline)
	res.Label = *label
	res.Rate = "closed-loop"
	if *rate > 0 {
		res.Rate = fmt.Sprintf("%.0f/s", *rate)
	}
	res.Concurrency = *conc
	res.Mismatches = mismatches.Load()
	res.HashChecked = hashed.Load()
	res.ErrorSamples = errSamples
	if rdb != nil {
		after := commandStats(rdb)
		res.RedisCmds = map[string]int64{}
		var total int64
		for k, v := range after {
			if d := v - before[k]; d > 0 && !isAdminCmd(k) {
				res.RedisCmds[k] = d
				total += d
			}
		}
		if res.Requests > 0 {
			res.RedisPerReq = float64(total) / float64(res.Requests)
		}
	}
	s3BytesMu.Lock()
	for i := range res.Timeline {
		res.Timeline[i].S3Bytes = s3Bytes[res.Timeline[i].Second]
	}
	for _, v := range s3Bytes {
		res.MaxS3Bytes = max(res.MaxS3Bytes, v)
	}
	s3BytesMu.Unlock()

	printResult(res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if res.Mismatches > 0 {
		os.Exit(2)
	}
}

func fetch(ctx context.Context, c *http.Client, base string, o object, verify bool, hashed *atomic.Int64) (sample, error) {
	s := sample{}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+o.Key, nil)
	var first time.Time
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotFirstResponseByte: func() { first = time.Now() },
	}))
	t0 := time.Now()
	resp, err := c.Do(req)
	if err != nil {
		s.total = time.Since(t0)
		return s, err
	}
	defer resp.Body.Close()
	s.tier = resp.Header.Get("X-Storage-Tier")
	var n int64
	var sum string
	if verify {
		h := sha256.New()
		n, err = io.Copy(h, resp.Body)
		sum = hex.EncodeToString(h.Sum(nil))
		hashed.Add(1)
	} else {
		n, err = io.Copy(io.Discard, resp.Body)
	}
	s.total = time.Since(t0)
	s.ttfb = first.Sub(t0)
	s.bytes = n
	switch {
	case err != nil:
		return s, err
	case resp.StatusCode != http.StatusOK:
		return s, fmt.Errorf("%s: status %d", o.Key, resp.StatusCode)
	case n != o.Size:
		return s, fmt.Errorf("%s: size mismatch %d != %d (tier %s)", o.Key, n, o.Size, s.tier)
	case verify && sum != o.Sum:
		return s, fmt.Errorf("%s: sha256 mismatch (tier %s)", o.Key, s.tier)
	}
	s.ok = true
	return s, nil
}

func percentiles(d []time.Duration) Percentiles {
	if len(d) == 0 {
		return Percentiles{}
	}
	slices.Sort(d)
	at := func(q float64) float64 {
		i := int(q * float64(len(d)-1))
		return float64(d[i].Microseconds()) / 1000
	}
	return Percentiles{P50: at(0.50), P90: at(0.90), P99: at(0.99), P999: at(0.999), Max: at(1)}
}

func summarize(all []sample, elapsed time.Duration, withTimeline bool) *Result {
	res := &Result{Duration: elapsed.Seconds(), Tiers: map[string]*TierStats{}}
	var ttfb, total []time.Duration
	byTier := map[string][2][]time.Duration{}
	var bytes int64
	buckets := map[int][]sample{}
	for _, s := range all {
		res.Requests++
		if withTimeline {
			sec := int(s.at / time.Second)
			buckets[sec] = append(buckets[sec], s)
		}
		if !s.ok {
			res.Errors++
			continue
		}
		bytes += s.bytes
		ttfb = append(ttfb, s.ttfb)
		total = append(total, s.total)
		bt := byTier[s.tier]
		bt[0] = append(bt[0], s.ttfb)
		bt[1] = append(bt[1], s.total)
		byTier[s.tier] = bt
	}
	res.RPS = float64(res.Requests) / elapsed.Seconds()
	res.MBps = float64(bytes) / (1 << 20) / elapsed.Seconds()
	res.TTFB = percentiles(ttfb)
	res.Total = percentiles(total)
	for tier, v := range byTier {
		res.Tiers[tier] = &TierStats{Requests: int64(len(v[0])), TTFB: percentiles(v[0]), Total: percentiles(v[1])}
	}
	if withTimeline {
		secs := make([]int, 0, len(buckets))
		for s := range buckets {
			secs = append(secs, s)
		}
		slices.Sort(secs)
		for _, sec := range secs {
			b := Bucket{Second: sec, RPS: float64(len(buckets[sec]))}
			var tt []time.Duration
			for _, s := range buckets[sec] {
				switch {
				case !s.ok:
					b.Errors++
				case s.tier == "s3":
					b.S3++
				default:
					b.Local++
				}
				if s.ok {
					tt = append(tt, s.ttfb)
				}
			}
			p := percentiles(tt)
			b.P50TTFB, b.P99TTFB, b.MaxTTFB = p.P50, p.P99, p.Max
			res.Timeline = append(res.Timeline, b)
		}
	}
	return res
}

func commandStats(rdb *redis.Client) map[string]int64 {
	info, err := rdb.Info(context.Background(), "commandstats").Result()
	if err != nil {
		log.Printf("redis INFO: %v", err)
		return nil
	}
	out := map[string]int64{}
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "cmdstat_") {
			continue
		}
		name, rest, _ := strings.Cut(strings.TrimPrefix(line, "cmdstat_"), ":")
		for _, kv := range strings.Split(rest, ",") {
			if v, ok := strings.CutPrefix(kv, "calls="); ok {
				out[name], _ = strconv.ParseInt(v, 10, 64)
			}
		}
	}
	return out
}

func isAdminCmd(name string) bool {
	switch name {
	case "info", "config|resetstat", "client|setinfo", "hello", "ping", "get":
		return true // our own sampling / connection setup
	}
	return false
}

func printResult(r *Result) {
	fmt.Printf("== %s  c=%d  %.1fs  rate=%s\n", r.Label, r.Concurrency, r.Duration, r.Rate)
	fmt.Printf("requests=%d rps=%.0f MB/s=%.1f errors=%d mismatches=%d hashed=%d\n",
		r.Requests, r.RPS, r.MBps, r.Errors, r.Mismatches, r.HashChecked)
	fmt.Printf("TTFB  ms p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f max=%.1f\n", r.TTFB.P50, r.TTFB.P90, r.TTFB.P99, r.TTFB.P999, r.TTFB.Max)
	fmt.Printf("Total ms p50=%.2f p90=%.2f p99=%.2f p99.9=%.2f max=%.1f\n", r.Total.P50, r.Total.P90, r.Total.P99, r.Total.P999, r.Total.Max)
	for tier, t := range r.Tiers {
		fmt.Printf("  tier=%-5s n=%-7d TTFB p50=%.2f p99=%.2f | total p50=%.2f p99=%.2f\n", tier, t.Requests, t.TTFB.P50, t.TTFB.P99, t.Total.P50, t.Total.P99)
	}
	if r.RedisCmds != nil {
		fmt.Printf("redis: %.4f cmds/request %v\n", r.RedisPerReq, r.RedisCmds)
	}
	if r.MaxS3Bytes > 0 {
		fmt.Printf("max S3 bytes observed: %d\n", r.MaxS3Bytes)
	}
	for _, b := range r.Timeline {
		fmt.Printf("  t=%3ds rps=%6.0f s3=%6d local=%6d err=%4d ttfb p50=%7.2f p99=%8.2f max=%8.1f s3bytes=%d\n",
			b.Second, b.RPS, b.S3, b.Local, b.Errors, b.P50TTFB, b.P99TTFB, b.MaxTTFB, b.S3Bytes)
	}
	for _, e := range r.ErrorSamples {
		fmt.Println("  error:", e)
	}
}
