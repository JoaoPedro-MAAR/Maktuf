package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

const (
	latencyMetric  = "end_to_end_latency_seconds"
	receivedMetric = "messages_received_total"
)

// appliedFault é uma falha como ela aconteceu.
type appliedFault struct {
	Fault
	when time.Duration
	proc string
	err  error
}

// result é a avaliação de uma meta.
type result struct {
	goal     Goal
	measured string
	target   string
	ok       bool
	detail   string
}

// sensorSummary é o "resumo final" que cada processo de sensores loga ao
// encerrar, depois de drenar o buffer. Os sensores já saíram quando as metas
// são avaliadas, então o log é a fonte do backlog final.
type sensorSummary struct {
	measured, confirmed, pending, noSubscribers, overflow, connectFailures float64
	found                                                                  bool
}

// readSensorSummary extrai os números da última linha "resumo final" do log.
func readSensorSummary(path string) (sensorSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return sensorSummary{}, err
	}
	defer func() { _ = f.Close() }()
	var line string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		if strings.Contains(sc.Text(), `msg="resumo final"`) {
			line = sc.Text()
		}
	}
	if err := sc.Err(); err != nil {
		return sensorSummary{}, err
	}
	if line == "" {
		return sensorSummary{}, nil
	}
	fields := map[string]float64{}
	for _, kv := range strings.Fields(line) {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			fields[k] = n
		}
	}
	return sensorSummary{
		measured:        fields["medidas"],
		confirmed:       fields["confirmadas_buffer"],
		pending:         fields["pendentes"],
		noSubscribers:   fields["sem_inscritos"],
		overflow:        fields["descartes_overflow"],
		connectFailures: fields["falhas_conexao"],
		found:           true,
	}, nil
}

// evaluator avalia as metas de um cenário com o que foi coletado.
type evaluator struct {
	s       *Scenario
	col     *collector
	faults  []appliedFault
	sensors map[string]sensorSummary
	procs   []string
}

func (e *evaluator) evaluate(g Goal) result {
	r := result{goal: g}
	switch g.Type {
	case goalLatency:
		e.latency(g, &r)
	case goalLatencyRatio:
		e.latencyRatio(g, &r)
	case goalThroughput:
		e.throughput(g, &r)
	case goalNoLoss:
		e.noLoss(&r)
	case goalRTO:
		e.rto(g, &r)
	case goalMetric:
		e.metric(g, &r)
	}
	return r
}

// windowQuantile estima o percentil q das observações que entraram no
// histograma entre a e b (diferença das contagens acumuladas).
func (e *evaluator) windowQuantile(proc, group string, q float64, a, b time.Duration) (float64, uint64, error) {
	labels := map[string]string{"group": group}
	bounds, end, ok := e.col.histogramAt(proc, latencyMetric, labels, b)
	if !ok {
		return 0, 0, fmt.Errorf("sem latencias do grupo %s ate %s", group, b)
	}
	delta := append([]uint64(nil), end...)
	if _, start, ok := e.col.histogramAt(proc, latencyMetric, labels, a); ok && len(start) == len(end) {
		for i := range delta {
			delta[i] -= min(start[i], delta[i])
		}
	}
	total := delta[len(delta)-1]
	if total == 0 {
		return 0, 0, fmt.Errorf("nenhuma leitura do grupo %s entre %s e %s", group, a, b)
	}
	return metrics.EstimateQuantile(q, bounds, delta), total, nil
}

func (e *evaluator) latency(g Goal, r *result) {
	a, b := g.Window[0].D(), g.Window[1].D()
	r.target = fmt.Sprintf("p%s <= %s", pct(g.Quantile), secs(*g.Max))
	v, n, err := e.windowQuantile(e.s.consumerProc(g.Process), g.Group, g.Quantile, a, b)
	if err != nil {
		r.measured, r.detail = "sem dados", err.Error()
		return
	}
	r.measured = fmt.Sprintf("p%s = %s", pct(g.Quantile), secs(v))
	r.detail = fmt.Sprintf("%d leituras do grupo %s entre %s e %s", n, g.Group, a, b)
	r.ok = v <= *g.Max
}

func (e *evaluator) latencyRatio(g Goal, r *result) {
	proc := e.s.consumerProc(g.Process)
	r.target = fmt.Sprintf("p%s(teste) / p%s(base) <= %g", pct(g.Quantile), pct(g.Quantile), g.MaxRatio)
	if g.MaxExtra > 0 {
		r.target = fmt.Sprintf("p%s(teste) <= %g x p%s(base) + %g s", pct(g.Quantile), g.MaxRatio, pct(g.Quantile), g.MaxExtra)
	}
	base, nb, err := e.windowQuantile(proc, g.Group, g.Quantile, g.Base[0].D(), g.Base[1].D())
	if err != nil {
		r.measured, r.detail = "sem dados", "base: "+err.Error()
		return
	}
	test, nt, err := e.windowQuantile(proc, g.Group, g.Quantile, g.Test[0].D(), g.Test[1].D())
	if err != nil {
		r.measured, r.detail = "sem dados", "teste: "+err.Error()
		return
	}
	if base <= 0 {
		r.measured, r.detail = "base zero", "p da linha de base estimado em 0"
		return
	}
	ratio := test / base
	limit := g.MaxRatio*base + g.MaxExtra
	r.measured = fmt.Sprintf("%.2fx (base %s, teste %s)", ratio, secs(base), secs(test))
	r.detail = fmt.Sprintf("%d leituras na base, %d no teste; limite %s", nb, nt, secs(limit))
	r.ok = test <= limit
}

func (e *evaluator) throughput(g Goal, r *result) {
	proc := e.s.consumerProc(g.Process)
	a, b := g.Window[0].D(), g.Window[1].D()
	v := e.col.sumAt(proc, receivedMetric, nil, b) - e.col.sumAt(proc, receivedMetric, nil, a)
	rate := v / (b - a).Seconds()
	r.target = fmt.Sprintf(">= %g leituras/s", *g.Min)
	r.measured = fmt.Sprintf("%.2f leituras/s", rate)
	r.detail = fmt.Sprintf("%.0f leituras recebidas entre %s e %s", v, a, b)
	r.ok = rate >= *g.Min
}

// noLoss: nenhum backlog nos sensores, nenhuma lacuna sem marca e cada
// consumidor recebeu tudo que os sensores tiveram confirmado com inscritos
// (RNF-07, RF-45).
func (e *evaluator) noLoss(r *result) {
	r.target = "pendentes = 0, lacunas sem marca = 0, recebidas >= confirmadas"
	var confirmed, pending float64
	for group, s := range e.sensors {
		if !s.found {
			r.measured, r.detail = "sem resumo", "sensores "+group+" nao logaram o resumo final"
			return
		}
		confirmed += s.confirmed - s.noSubscribers
		pending += s.pending
	}
	r.ok = pending == 0
	var parts []string
	for _, c := range e.s.Consumers {
		proc := "consumidor:" + c.ClientID
		received := e.col.sumLast(proc, receivedMetric, nil)
		gaps := e.col.sumLast(proc, "missing_readings_total", map[string]string{"cause": "unmarked"})
		if received < confirmed || gaps != 0 {
			r.ok = false
		}
		parts = append(parts, fmt.Sprintf("%s: recebidas %.0f, lacunas sem marca %.0f", c.ClientID, received, gaps))
	}
	r.measured = fmt.Sprintf("confirmadas %.0f, pendentes %.0f", confirmed, pending)
	r.detail = strings.Join(parts, "; ")
}

// rto mede da falha até o consumidor voltar a receber leituras novas. A
// primeira coleta depois da falha é ignorada (mensagens já em trânsito); a
// precisão é o scrape_interval.
func (e *evaluator) rto(g Goal, r *result) {
	r.target = fmt.Sprintf("<= %s", secs(*g.Max))
	if g.Fault >= len(e.faults) || e.faults[g.Fault].err != nil {
		r.measured, r.detail = "falha nao aplicada", "a falha da meta nao foi aplicada"
		return
	}
	f := e.faults[g.Fault]
	proc := e.s.consumerProc(g.Process)
	start := f.when + e.s.ScrapeInterval.D()
	base := e.col.sumAt(proc, receivedMetric, nil, start)
	for _, t := range e.col.timesAfter(start) {
		if e.col.sumAt(proc, receivedMetric, nil, t) > base {
			rto := t - f.when
			r.measured = secs(rto.Seconds())
			r.detail = fmt.Sprintf("%s em %s (%s) ate a primeira entrega nova", f.Action, f.proc, f.when.Round(time.Millisecond))
			r.ok = rto.Seconds() <= *g.Max
			return
		}
	}
	r.measured, r.detail = "sem retomada", "o consumidor nao voltou a receber ate o fim do cenario"
}

func (e *evaluator) metric(g Goal, r *result) {
	procs := e.expand(g.Process)
	var v float64
	if g.Window != nil {
		a, b := g.Window[0].D(), g.Window[1].D()
		for _, p := range procs {
			v += e.col.sumAt(p, g.Metric, g.Labels, b) - e.col.sumAt(p, g.Metric, g.Labels, a)
		}
		r.detail = fmt.Sprintf("variacao entre %s e %s", a, b)
	} else {
		for _, p := range procs {
			v += e.col.sumLast(p, g.Metric, g.Labels)
		}
		r.detail = "ultimo valor coletado"
	}
	r.measured = strconv.FormatFloat(v, 'g', -1, 64)
	r.ok = true
	var target []string
	if g.Min != nil {
		target = append(target, fmt.Sprintf(">= %g", *g.Min))
		r.ok = r.ok && v >= *g.Min
	}
	if g.Max != nil {
		target = append(target, fmt.Sprintf("<= %g", *g.Max))
		r.ok = r.ok && v <= *g.Max
	}
	r.target = strings.Join(target, " e ")
}

// expand traduz process para a lista de processos: "brokers" soma b1 e b2.
func (e *evaluator) expand(process string) []string {
	if process == "brokers" {
		var out []string
		for _, p := range e.procs {
			if p == "b1" || p == "b2" {
				out = append(out, p)
			}
		}
		return out
	}
	return []string{process}
}

func pct(q float64) string { return strconv.FormatFloat(q*100, 'g', -1, 64) }

func secs(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	return strconv.FormatFloat(v, 'f', 3, 64) + " s"
}

// timesAfter devolve os instantes de coleta depois de t.
func (c *collector) timesAfter(t time.Duration) []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []time.Duration
	for _, x := range c.times {
		if x > t {
			out = append(out, x)
		}
	}
	return out
}
