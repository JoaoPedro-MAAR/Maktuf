// Package metrics coleta as métricas do Maktuf (requisitos.md, seção 9) e as
// expõe no formato textual do Prometheus, usando só a biblioteca padrão.
//
// Há dois jeitos de registrar uma métrica:
//
//   - Counter, Gauge e Histogram guardam o próprio valor e são atualizados
//     no ponto do código onde o evento acontece.
//   - NewCounterFunc e NewGaugeFunc calculam o valor na hora da leitura, a
//     partir de um estado que já existe em outro lugar (profundidade de fila,
//     buffer do sensor), sem duplicá-lo.
//
// Todos os tipos podem ser usados por várias goroutines.
package metrics

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tipos de família, como aparecem na linha # TYPE.
const (
	TypeCounter   = "counter"
	TypeGauge     = "gauge"
	TypeHistogram = "histogram"
)

// QuantileSuffix forma o nome da família extra que cada histograma expõe com
// os percentis estimados pelos buckets.
const QuantileSuffix = "_quantile"

// Quantiles são os percentis publicados para cada histograma.
var Quantiles = []float64{0.5, 0.95, 0.99}

// DefaultLatencyBuckets vai de 1 ms a 1 h: cobre a latência dentro do broker
// e a latência fim a fim de leituras que ficaram no buffer do sensor.
var DefaultLatencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
	1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
}

// Label é um par nome=valor de uma série.
type Label struct {
	Name  string
	Value string
}

// Sample é uma linha da exposição. Suffix é "", "_bucket", "_sum" ou
// "_count".
type Sample struct {
	Suffix string
	Labels []Label
	Value  float64
}

// Family é uma métrica com todas as suas séries.
type Family struct {
	Name    string
	Help    string
	Type    string
	Samples []Sample
}

var (
	metricNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	labelNameRE  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

type collector interface {
	samples() []Sample
}

type family struct {
	name       string
	help       string
	typ        string
	labelNames []string
	collector  collector
	hist       *Histogram
}

// Registry guarda as famílias registradas.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
	reserved map[string]struct{}
}

// NewRegistry cria um registro vazio.
func NewRegistry() *Registry {
	return &Registry{
		families: make(map[string]*family),
		reserved: make(map[string]struct{}),
	}
}

// register valida e guarda a família. Nome inválido ou repetido é erro de
// programação e entra em pânico, como o MustRegister do Prometheus.
func (r *Registry) register(f *family, extraNames ...string) {
	if !metricNameRE.MatchString(f.name) {
		panic(fmt.Sprintf("metrics: nome de metrica invalido %q", f.name))
	}
	seen := make(map[string]struct{}, len(f.labelNames))
	for _, l := range f.labelNames {
		if !labelNameRE.MatchString(l) || strings.HasPrefix(l, "__") {
			panic(fmt.Sprintf("metrics: rotulo invalido %q em %s", l, f.name))
		}
		if f.typ == TypeHistogram && (l == "le" || l == "quantile") {
			panic(fmt.Sprintf("metrics: rotulo %q reservado em histograma %s", l, f.name))
		}
		if _, dup := seen[l]; dup {
			panic(fmt.Sprintf("metrics: rotulo %q repetido em %s", l, f.name))
		}
		seen[l] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, name := range append([]string{f.name}, extraNames...) {
		if _, dup := r.reserved[name]; dup {
			panic(fmt.Sprintf("metrics: metrica %q ja registrada", name))
		}
	}
	for _, name := range append([]string{f.name}, extraNames...) {
		r.reserved[name] = struct{}{}
	}
	r.families[f.name] = f
}

// NewCounter registra um contador com os rótulos informados.
func (r *Registry) NewCounter(name, help string, labelNames ...string) *Counter {
	c := &Counter{vec: newVec(labelNames, func() *atomic.Uint64 { return new(atomic.Uint64) })}
	r.register(&family{name: name, help: help, typ: TypeCounter, labelNames: labelNames, collector: c})
	return c
}

// NewGauge registra um medidor com os rótulos informados.
func (r *Registry) NewGauge(name, help string, labelNames ...string) *Gauge {
	g := &Gauge{vec: newVec(labelNames, func() *atomicFloat { return new(atomicFloat) })}
	r.register(&family{name: name, help: help, typ: TypeGauge, labelNames: labelNames, collector: g})
	return g
}

// NewHistogram registra um histograma. Buckets são os limites superiores em
// ordem crescente; nil usa DefaultLatencyBuckets. O bucket +Inf é implícito.
// O nome <name>_quantile fica reservado para os percentis estimados.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labelNames ...string) *Histogram {
	if buckets == nil {
		buckets = DefaultLatencyBuckets
	}
	for i, b := range buckets {
		if math.IsNaN(b) || math.IsInf(b, 0) || (i > 0 && b <= buckets[i-1]) {
			panic(fmt.Sprintf("metrics: buckets de %s devem ser finitos e crescentes", name))
		}
	}
	bounds := append([]float64(nil), buckets...)
	h := &Histogram{
		bounds: bounds,
		vec: newVec(labelNames, func() *histSeries {
			return &histSeries{counts: make([]atomic.Uint64, len(bounds)+1)}
		}),
	}
	r.register(&family{name: name, help: help, typ: TypeHistogram, labelNames: labelNames, collector: h, hist: h},
		name+QuantileSuffix)
	return h
}

// NewCounterFunc registra um contador cujo valor é calculado por collect a
// cada leitura. Os rótulos de cada Sample devem seguir labelNames, na mesma
// ordem; amostras fora disso são ignoradas.
func (r *Registry) NewCounterFunc(name, help string, labelNames []string, collect func() []Sample) {
	r.register(&family{name: name, help: help, typ: TypeCounter, labelNames: labelNames,
		collector: funcCollector{labelNames: labelNames, collect: collect}})
}

// NewGaugeFunc registra um medidor calculado por collect a cada leitura, com
// as mesmas regras de NewCounterFunc.
func (r *Registry) NewGaugeFunc(name, help string, labelNames []string, collect func() []Sample) {
	r.register(&family{name: name, help: help, typ: TypeGauge, labelNames: labelNames,
		collector: funcCollector{labelNames: labelNames, collect: collect}})
}

// Gather fotografa todas as famílias, em ordem de nome e com as séries em
// ordem de rótulo. Cada histograma também gera a família <nome>_quantile com
// p50, p95 e p99 de cada série.
func (r *Registry) Gather() []Family {
	r.mu.Lock()
	fams := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		fams = append(fams, f)
	}
	r.mu.Unlock()

	out := make([]Family, 0, len(fams))
	for _, f := range fams {
		out = append(out, Family{Name: f.name, Help: f.help, Type: f.typ, Samples: f.collector.samples()})
		if f.hist != nil {
			out = append(out, Family{
				Name:    f.name + QuantileSuffix,
				Help:    "Percentis de " + f.name + " estimados pelos buckets.",
				Type:    TypeGauge,
				Samples: f.hist.quantileSamples(),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// vec guarda as séries de uma métrica, uma por combinação de valores de
// rótulo.
type vec[T any] struct {
	labelNames []string
	newSeries  func() *T

	mu     sync.RWMutex
	series map[string]*T
}

func newVec[T any](labelNames []string, newSeries func() *T) vec[T] {
	return vec[T]{
		labelNames: append([]string(nil), labelNames...),
		newSeries:  newSeries,
		series:     make(map[string]*T),
	}
}

// keySep não aparece em texto válido; separa os valores de rótulo na chave.
const keySep = "\xff"

func seriesKey(values []string) string { return strings.Join(values, keySep) }

func splitKey(key string, n int) []string {
	if n == 0 {
		return nil
	}
	return strings.SplitN(key, keySep, n)
}

// get devolve a série dos valores informados, criando-a se preciso.
func (v *vec[T]) get(values []string) *T {
	if len(values) != len(v.labelNames) {
		panic(fmt.Sprintf("metrics: %d valores de rotulo, esperado %d (%v)", len(values), len(v.labelNames), v.labelNames))
	}
	key := seriesKey(values)
	v.mu.RLock()
	s, ok := v.series[key]
	v.mu.RUnlock()
	if ok {
		return s
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if s, ok = v.series[key]; !ok {
		s = v.newSeries()
		v.series[key] = s
	}
	return s
}

// each percorre as séries em ordem de chave.
func (v *vec[T]) each(fn func(labels []Label, s *T)) {
	v.mu.RLock()
	keys := make([]string, 0, len(v.series))
	for k := range v.series {
		keys = append(keys, k)
	}
	series := make(map[string]*T, len(keys))
	for _, k := range keys {
		series[k] = v.series[k]
	}
	v.mu.RUnlock()

	sort.Strings(keys)
	for _, k := range keys {
		fn(v.labels(splitKey(k, len(v.labelNames))), series[k])
	}
}

func (v *vec[T]) labels(values []string) []Label {
	out := make([]Label, len(values))
	for i, val := range values {
		out[i] = Label{Name: v.labelNames[i], Value: val}
	}
	return out
}

// Counter é um contador monotônico.
type Counter struct {
	vec vec[atomic.Uint64]
}

// Inc soma 1 à série dos valores de rótulo informados.
func (c *Counter) Inc(labelValues ...string) { c.vec.get(labelValues).Add(1) }

// Add soma n à série dos valores de rótulo informados.
func (c *Counter) Add(n uint64, labelValues ...string) { c.vec.get(labelValues).Add(n) }

// Value devolve o valor atual da série.
func (c *Counter) Value(labelValues ...string) uint64 { return c.vec.get(labelValues).Load() }

func (c *Counter) samples() []Sample {
	var out []Sample
	c.vec.each(func(labels []Label, s *atomic.Uint64) {
		out = append(out, Sample{Labels: labels, Value: float64(s.Load())})
	})
	return out
}

// atomicFloat guarda os bits de um float64 para atualizar sem lock.
type atomicFloat struct {
	bits atomic.Uint64
}

func (f *atomicFloat) load() float64 { return math.Float64frombits(f.bits.Load()) }

func (f *atomicFloat) store(v float64) { f.bits.Store(math.Float64bits(v)) }

func (f *atomicFloat) add(v float64) {
	for {
		old := f.bits.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if f.bits.CompareAndSwap(old, next) {
			return
		}
	}
}

// Gauge é um valor que sobe e desce.
type Gauge struct {
	vec vec[atomicFloat]
}

// Set define o valor da série.
func (g *Gauge) Set(v float64, labelValues ...string) { g.vec.get(labelValues).store(v) }

// Add soma v (que pode ser negativo) ao valor da série.
func (g *Gauge) Add(v float64, labelValues ...string) { g.vec.get(labelValues).add(v) }

// Value devolve o valor atual da série.
func (g *Gauge) Value(labelValues ...string) float64 { return g.vec.get(labelValues).load() }

func (g *Gauge) samples() []Sample {
	var out []Sample
	g.vec.each(func(labels []Label, s *atomicFloat) {
		out = append(out, Sample{Labels: labels, Value: s.load()})
	})
	return out
}

// histSeries conta observações por bucket (não cumulativo; o último é +Inf).
// A contagem total é a soma dos buckets, para a exposição ficar coerente
// mesmo com observações simultâneas à leitura.
type histSeries struct {
	counts []atomic.Uint64
	sum    atomicFloat
}

func (s *histSeries) snapshot() (cumulative []uint64, sum float64) {
	cumulative = make([]uint64, len(s.counts))
	var acc uint64
	for i := range s.counts {
		acc += s.counts[i].Load()
		cumulative[i] = acc
	}
	return cumulative, s.sum.load()
}

// Histogram distribui observações em buckets.
type Histogram struct {
	bounds []float64
	vec    vec[histSeries]
}

// Observe registra o valor v na série dos rótulos informados.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	s := h.vec.get(labelValues)
	i := sort.SearchFloat64s(h.bounds, v) // primeiro limite >= v; len = +Inf
	s.counts[i].Add(1)
	s.sum.add(v)
}

// ObserveDuration registra d em segundos.
func (h *Histogram) ObserveDuration(d time.Duration, labelValues ...string) {
	h.Observe(d.Seconds(), labelValues...)
}

// Count devolve o total de observações da série.
func (h *Histogram) Count(labelValues ...string) uint64 {
	cumulative, _ := h.vec.get(labelValues).snapshot()
	return cumulative[len(cumulative)-1]
}

// Quantile estima o percentil q (entre 0 e 1) da série.
func (h *Histogram) Quantile(q float64, labelValues ...string) float64 {
	cumulative, _ := h.vec.get(labelValues).snapshot()
	return estimateQuantile(q, h.bounds, cumulative)
}

// estimateQuantile segue a regra do histogram_quantile do Prometheus: acha o
// bucket onde cai a posição q·total e interpola linearmente dentro dele,
// supondo observações espalhadas por igual no intervalo. Sem observações devolve NaN; no
// bucket +Inf devolve o maior limite finito.
func estimateQuantile(q float64, bounds []float64, cumulative []uint64) float64 {
	total := cumulative[len(cumulative)-1]
	if total == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	q = math.Max(0, math.Min(1, q))
	rank := q * float64(total)

	i := sort.Search(len(cumulative), func(i int) bool { return float64(cumulative[i]) >= rank })
	if i >= len(bounds) {
		return bounds[len(bounds)-1]
	}

	lower, prev := 0.0, uint64(0)
	if i > 0 {
		lower, prev = bounds[i-1], cumulative[i-1]
	} else if bounds[0] <= 0 {
		return bounds[0]
	}
	inBucket := cumulative[i] - prev
	if inBucket == 0 {
		return bounds[i]
	}
	return lower + (bounds[i]-lower)*(rank-float64(prev))/float64(inBucket)
}

func (h *Histogram) samples() []Sample {
	var out []Sample
	h.vec.each(func(labels []Label, s *histSeries) {
		cumulative, sum := s.snapshot()
		for i, c := range cumulative {
			le := "+Inf"
			if i < len(h.bounds) {
				le = formatFloat(h.bounds[i])
			}
			out = append(out, Sample{Suffix: "_bucket", Labels: withLabel(labels, "le", le), Value: float64(c)})
		}
		out = append(out,
			Sample{Suffix: "_sum", Labels: labels, Value: sum},
			Sample{Suffix: "_count", Labels: labels, Value: float64(cumulative[len(cumulative)-1])},
		)
	})
	return out
}

func (h *Histogram) quantileSamples() []Sample {
	var out []Sample
	h.vec.each(func(labels []Label, s *histSeries) {
		cumulative, _ := s.snapshot()
		for _, q := range Quantiles {
			out = append(out, Sample{
				Labels: withLabel(labels, "quantile", formatFloat(q)),
				Value:  estimateQuantile(q, h.bounds, cumulative),
			})
		}
	})
	return out
}

func withLabel(labels []Label, name, value string) []Label {
	out := make([]Label, len(labels), len(labels)+1)
	copy(out, labels)
	return append(out, Label{Name: name, Value: value})
}

type funcCollector struct {
	labelNames []string
	collect    func() []Sample
}

func (f funcCollector) samples() []Sample {
	raw := f.collect()
	out := make([]Sample, 0, len(raw))
	for _, s := range raw {
		if !sameLabelNames(s.Labels, f.labelNames) {
			continue
		}
		out = append(out, Sample{Labels: s.Labels, Value: s.Value})
	}
	sort.SliceStable(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return out
}

func sameLabelNames(labels []Label, names []string) bool {
	if len(labels) != len(names) {
		return false
	}
	for i, l := range labels {
		if l.Name != names[i] {
			return false
		}
	}
	return true
}

func labelKey(labels []Label) string {
	values := make([]string, len(labels))
	for i, l := range labels {
		values[i] = l.Value
	}
	return seriesKey(values)
}
