package main

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

type point struct {
	t time.Duration
	v float64
}

// collector guarda as amostras do /metrics de todos os processos ao longo do
// tempo (t contado do início dos sensores) e as grava em metricas.csv.
// Séries por sensor (rótulo sensor_id) são somadas por processo para manter a
// cardinalidade baixa.
type collector struct {
	start time.Time

	mu     sync.Mutex
	series map[string][]point
	times  []time.Duration
	csv    *bufio.Writer
	file   *os.File
}

func newCollector(path string, start time.Time) (*collector, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)
	if _, err := w.WriteString("t_s,processo,metrica,rotulos,valor\n"); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &collector{start: start, series: map[string][]point{}, csv: w, file: f}, nil
}

func (c *collector) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.csv.Flush(); err != nil {
		_ = c.file.Close()
		return err
	}
	return c.file.Close()
}

// seriesKey identifica uma série: processo, nome (com sufixo) e rótulos em
// ordem alfabética.
func seriesKey(proc, name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(proc)
	b.WriteByte('|')
	b.WriteString(name)
	b.WriteByte('|')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

// scrapeAll lê todos os processos vivos e registra as amostras no instante
// atual.
func (c *collector) scrapeAll(cl *cluster) {
	samples := map[string]float64{}
	for _, name := range cl.order {
		p := cl.procs[name]
		if !p.running() {
			continue
		}
		fams, err := p.scrape(time.Second)
		if err != nil {
			continue
		}
		for _, f := range fams {
			if strings.HasSuffix(f.Name, metrics.QuantileSuffix) {
				continue
			}
			for _, s := range f.Samples {
				labels := map[string]string{}
				for _, l := range s.Labels {
					if l.Name == "sensor_id" || l.Name == "topic" {
						continue
					}
					labels[l.Name] = l.Value
				}
				samples[seriesKey(name, f.Name+s.Suffix, labels)] += s.Value
			}
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// O instante é tomado sob o lock: coletas simultâneas (periódica e de
	// drenagem) ficam em ordem crescente, como as buscas exigem.
	t := time.Since(c.start)
	if n := len(c.times); n > 0 && t <= c.times[n-1] {
		t = c.times[n-1] + time.Nanosecond
	}
	c.times = append(c.times, t)
	keys := make([]string, 0, len(samples))
	for k := range samples {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := samples[k]
		c.series[k] = append(c.series[k], point{t: t, v: v})
		parts := strings.SplitN(k, "|", 3)
		fmt.Fprintf(c.csv, "%.3f,%s,%s,\"%s\",%s\n", t.Seconds(), parts[0], parts[1],
			strings.ReplaceAll(parts[2], `"`, `""`), strconv.FormatFloat(v, 'g', -1, 64))
	}
}

// run coleta a cada intervalo até ctx terminar.
func (c *collector) run(ctx context.Context, cl *cluster, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	c.scrapeAll(cl)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.scrapeAll(cl)
		}
	}
}

// at devolve o valor da série na última coleta em ou antes de t; ok falso se
// não houver.
func (c *collector) at(proc, name string, labels map[string]string, t time.Duration) (float64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pts := c.series[seriesKey(proc, name, labels)]
	i := sort.Search(len(pts), func(i int) bool { return pts[i].t > t })
	if i == 0 {
		return 0, false
	}
	return pts[i-1].v, true
}

// points devolve a série inteira.
func (c *collector) points(proc, name string, labels map[string]string) []point {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]point(nil), c.series[seriesKey(proc, name, labels)]...)
}

// last devolve o último valor da série.
func (c *collector) last(proc, name string, labels map[string]string) (float64, bool) {
	return c.at(proc, name, labels, time.Duration(math.MaxInt64))
}

// sumLast soma o último valor de todas as séries de um processo com esse nome
// que contêm os rótulos pedidos (por exemplo, todos os grupos).
func (c *collector) sumLast(proc, name string, labels map[string]string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := proc + "|" + name + "|"
	var total float64
	for k, pts := range c.series {
		if !strings.HasPrefix(k, prefix) || len(pts) == 0 || !keyHasLabels(k[len(prefix):], labels) {
			continue
		}
		total += pts[len(pts)-1].v
	}
	return total
}

// sumAt é o sumLast num instante.
func (c *collector) sumAt(proc, name string, labels map[string]string, t time.Duration) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := proc + "|" + name + "|"
	var total float64
	for k, pts := range c.series {
		if !strings.HasPrefix(k, prefix) || !keyHasLabels(k[len(prefix):], labels) {
			continue
		}
		i := sort.Search(len(pts), func(i int) bool { return pts[i].t > t })
		if i > 0 {
			total += pts[i-1].v
		}
	}
	return total
}

func keyHasLabels(encoded string, want map[string]string) bool {
	for k, v := range want {
		if !strings.Contains(","+encoded+",", ","+k+"="+v+",") {
			return false
		}
	}
	return true
}

// histogramAt devolve os limites e as contagens acumuladas (com +Inf no fim)
// de um histograma na última coleta em ou antes de t.
func (c *collector) histogramAt(proc, name string, labels map[string]string, t time.Duration) ([]float64, []uint64, bool) {
	c.mu.Lock()
	prefix := proc + "|" + name + "_bucket|"
	type bucket struct {
		le float64
		v  float64
	}
	var buckets []bucket
	for k, pts := range c.series {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		enc := k[len(prefix):]
		le, rest := splitLe(enc)
		if le == "" || !sameLabels(rest, labels) {
			continue
		}
		bound, err := strconv.ParseFloat(le, 64)
		if err != nil {
			continue
		}
		i := sort.Search(len(pts), func(i int) bool { return pts[i].t > t })
		if i == 0 {
			continue
		}
		buckets = append(buckets, bucket{le: bound, v: pts[i-1].v})
	}
	c.mu.Unlock()
	if len(buckets) == 0 {
		return nil, nil, false
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].le < buckets[j].le })
	bounds := make([]float64, 0, len(buckets)-1)
	cumulative := make([]uint64, len(buckets))
	for i, b := range buckets {
		if !math.IsInf(b.le, 1) {
			bounds = append(bounds, b.le)
		}
		cumulative[i] = uint64(b.v)
	}
	if len(bounds) != len(buckets)-1 {
		return nil, nil, false
	}
	return bounds, cumulative, true
}

// splitLe separa o rótulo le dos demais numa chave codificada.
func splitLe(enc string) (string, map[string]string) {
	rest := map[string]string{}
	var le string
	for _, kv := range strings.Split(enc, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if k == "le" {
			le = v
			continue
		}
		rest[k] = v
	}
	return le, rest
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
