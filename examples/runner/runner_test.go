package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeScenario(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cenario.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

const validScenario = `{
  "name": "teste", "duration": "10s",
  "cluster": {"replicated": true, "judge": true},
  "sensors": [{"group": "sensor", "count": 2, "continuous_ratio": 1}],
  "faults": [{"at": "5s", "action": "kill", "target": "primario"}],
  "goals": [
    {"name": "rto", "type": "rto", "fault": 0, "max": 30},
    {"name": "lat", "type": "latency", "group": "sensor", "quantile": 0.95, "window": ["1s", "9s"], "max": 2}
  ]
}`

func TestLoadAppliesDefaultsAndValidates(t *testing.T) {
	s, err := Load(writeScenario(t, validScenario))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if s.Duration.D() != 10*time.Second || s.ScrapeInterval.D() != defaultScrapeInterval || s.Consumers[0].ClientID != defaultConsumerID {
		t.Fatalf("cenario = %+v", s)
	}
}

func TestLoadRejectsInvalidScenarios(t *testing.T) {
	cases := map[string]string{
		"campo desconhecido":    strings.Replace(validScenario, `"name": "teste"`, `"name": "teste", "durasao": "1s"`, 1),
		"falha fora da duracao": strings.Replace(validScenario, `"at": "5s"`, `"at": "20s"`, 1),
		"alvo invalido":         strings.Replace(validScenario, `"target": "primario"`, `"target": "b3"`, 1),
		"juiz sem replicacao":   strings.Replace(validScenario, `"replicated": true`, `"replicated": false`, 1),
		"meta desconhecida":     strings.Replace(validScenario, `"type": "rto"`, `"type": "rpo"`, 1),
		"janela invertida":      strings.Replace(validScenario, `["1s", "9s"]`, `["9s", "1s"]`, 1),
		"duracao em numero":     strings.Replace(validScenario, `"duration": "10s"`, `"duration": 10`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeScenario(t, body)); err == nil {
				t.Fatalf("cenario invalido aceito")
			}
		})
	}
}

// Os cenários versionados precisam carregar e validar.
func TestShippedScenariosLoad(t *testing.T) {
	files, _ := filepath.Glob("../cenarios/*.json")
	ci, _ := filepath.Glob("../cenarios/ci/*.json")
	files = append(files, ci...)
	if len(files) < 14 {
		t.Fatalf("esperava 7 cenarios completos e 7 reduzidos, achei %d", len(files))
	}
	escala, _ := filepath.Glob("../cenarios/escala/*.json")
	if len(escala) == 0 {
		t.Fatal("nenhum cenario de escala em cenarios/escala")
	}
	files = append(files, escala...)
	for _, f := range files {
		if _, err := Load(f); err != nil {
			t.Errorf("%v", err)
		}
	}
}

// newTestCollector monta um collector com amostras sintéticas.
func newTestCollector(t *testing.T) *collector {
	t.Helper()
	c, err := newCollector(filepath.Join(t.TempDir(), "m.csv"), time.Now())
	if err != nil {
		t.Fatalf("newCollector: %v", err)
	}
	t.Cleanup(func() { _ = c.close() })
	return c
}

func (c *collector) add(proc, name string, labels map[string]string, t time.Duration, v float64) {
	key := seriesKey(proc, name, labels)
	c.series[key] = append(c.series[key], point{t: t, v: v})
	if n := len(c.times); n == 0 || c.times[n-1] < t {
		c.times = append(c.times, t)
	}
}

// addHistogram registra os buckets acumulados (limites 0.5 e 1) num instante.
func (c *collector) addHistogram(proc, group string, t time.Duration, le05, le1, inf float64) {
	for le, v := range map[string]float64{"0.5": le05, "1": le1, "+Inf": inf} {
		c.add(proc, latencyMetric+"_bucket", map[string]string{"group": group, "le": le}, t, v)
	}
}

func f(v float64) *float64 { return &v }

func dur(ds ...time.Duration) []Duration {
	out := make([]Duration, len(ds))
	for i, d := range ds {
		out[i] = Duration(d)
	}
	return out
}

func TestLatencyGoalUsesOnlyTheWindow(t *testing.T) {
	c := newTestCollector(t)
	p := "consumidor:c1"
	// Antes da janela: 10 leituras rápidas. Na janela: 10 em (0.5, 1].
	c.addHistogram(p, "sensor", time.Second, 10, 10, 10)
	c.addHistogram(p, "sensor", 2*time.Second, 10, 20, 20)
	s := &Scenario{Consumers: []ConsumerSpec{{ClientID: "c1"}}}
	e := &evaluator{s: s, col: c}

	r := e.evaluate(Goal{Name: "p50", Type: goalLatency, Group: "sensor", Quantile: 0.5,
		Window: dur(time.Second, 2*time.Second), Max: f(0.8)})
	if !r.ok || !strings.Contains(r.measured, "0.750") {
		t.Fatalf("p50 da janela = %+v, esperado 0.750 s", r)
	}
	r = e.evaluate(Goal{Name: "p50", Type: goalLatency, Group: "sensor", Quantile: 0.5,
		Window: dur(time.Second, 2*time.Second), Max: f(0.7)})
	if r.ok {
		t.Fatalf("p50 0.75 passou com max 0.7")
	}
}

func TestLatencyRatioGoal(t *testing.T) {
	c := newTestCollector(t)
	p := "consumidor:c1"
	c.addHistogram(p, "sensor", 0, 0, 0, 0)
	c.addHistogram(p, "sensor", 2*time.Second, 10, 10, 10) // base: tudo <= 0.5
	c.addHistogram(p, "sensor", 4*time.Second, 10, 20, 20) // teste: tudo em (0.5, 1]
	s := &Scenario{Consumers: []ConsumerSpec{{ClientID: "c1"}}}
	e := &evaluator{s: s, col: c}

	r := e.evaluate(Goal{Name: "justica", Type: goalLatencyRatio, Group: "sensor", Quantile: 0.95,
		Base: dur(0, 2*time.Second), Test: dur(2*time.Second, 4*time.Second), MaxRatio: 2})
	if r.ok {
		t.Fatalf("razao %s passou com max 2", r.measured)
	}
	if !strings.HasPrefix(r.measured, "2.05x") {
		t.Fatalf("razao = %s, esperado 2.05x (0.975/0.475)", r.measured)
	}

	// Com folga absoluta: limite = 2 * 0.475 + 0.05 = 1.0 s >= 0.975 s.
	r = e.evaluate(Goal{Name: "justica", Type: goalLatencyRatio, Group: "sensor", Quantile: 0.95,
		Base: dur(0, 2*time.Second), Test: dur(2*time.Second, 4*time.Second), MaxRatio: 2, MaxExtra: 0.05})
	if !r.ok {
		t.Fatalf("teste %s nao passou com limite 2x base + 0.05 s (%s)", r.measured, r.detail)
	}
	r = e.evaluate(Goal{Name: "justica", Type: goalLatencyRatio, Group: "sensor", Quantile: 0.95,
		Base: dur(0, 2*time.Second), Test: dur(2*time.Second, 4*time.Second), MaxRatio: 2, MaxExtra: 0.02})
	if r.ok {
		t.Fatalf("teste %s passou com limite 2x base + 0.02 s = 0.97 s", r.measured)
	}
}

func TestRTOGoal(t *testing.T) {
	c := newTestCollector(t)
	p := "consumidor:c1"
	labels := map[string]string{"group": "sensor"}
	values := []float64{0, 10, 20, 30, 30, 30, 30, 30, 40, 50}
	for i, v := range values {
		c.add(p, receivedMetric, labels, time.Duration(i)*500*time.Millisecond, v)
	}
	s := &Scenario{ScrapeInterval: Duration(500 * time.Millisecond), Consumers: []ConsumerSpec{{ClientID: "c1"}}}
	faults := []appliedFault{{Fault: Fault{Action: "kill", Target: "primario"}, when: 1600 * time.Millisecond, proc: "b1"}}
	e := &evaluator{s: s, col: c, faults: faults}

	r := e.evaluate(Goal{Name: "rto", Type: goalRTO, Fault: 0, Max: f(30)})
	// Base em 2.1 s (30); a primeira coleta maior é a de 4 s: RTO = 2.4 s.
	if !r.ok || r.measured != "2.400 s" {
		t.Fatalf("rto = %+v, esperado 2.400 s", r)
	}
	r = e.evaluate(Goal{Name: "rto", Type: goalRTO, Fault: 0, Max: f(2)})
	if r.ok {
		t.Fatalf("rto 2.4 s passou com max 2 s")
	}
}

func TestNoLossGoal(t *testing.T) {
	s := &Scenario{Consumers: []ConsumerSpec{{ClientID: "c1"}}}
	build := func(received, gaps float64, sensors sensorSummary) result {
		c := newTestCollector(t)
		c.add("consumidor:c1", receivedMetric, map[string]string{"group": "sensor"}, time.Second, received)
		c.add("consumidor:c1", "missing_readings_total", map[string]string{"cause": "unmarked"}, time.Second, gaps)
		e := &evaluator{s: s, col: c, sensors: map[string]sensorSummary{"sensor": sensors}}
		return e.evaluate(Goal{Name: "sem perda", Type: goalNoLoss})
	}
	ok := sensorSummary{confirmed: 10, found: true}
	if r := build(10, 0, ok); !r.ok {
		t.Fatalf("caso sem perda reprovado: %+v", r)
	}
	if r := build(9, 0, ok); r.ok {
		t.Fatalf("recebidas < confirmadas aprovado")
	}
	if r := build(10, 1, ok); r.ok {
		t.Fatalf("lacuna sem marca aprovada")
	}
	if r := build(10, 0, sensorSummary{confirmed: 10, pending: 2, found: true}); r.ok {
		t.Fatalf("backlog no sensor aprovado")
	}
	if r := build(10, 0, sensorSummary{}); r.ok {
		t.Fatalf("sem resumo dos sensores aprovado")
	}
}

func TestMetricGoalSumsBrokersAndWindows(t *testing.T) {
	c := newTestCollector(t)
	c.add("b1", "retention_drops_total", map[string]string{"client_id": "x"}, time.Second, 3)
	c.add("b2", "retention_drops_total", map[string]string{"client_id": "x"}, time.Second, 4)
	c.add("consumidor:c1", receivedMetric, nil, time.Second, 10)
	c.add("consumidor:c1", receivedMetric, nil, 3*time.Second, 25)
	e := &evaluator{s: &Scenario{}, col: c, procs: []string{"b1", "b2", "consumidor:c1"}}

	if r := e.evaluate(Goal{Name: "drops", Type: goalMetric, Process: "brokers", Metric: "retention_drops_total", Min: f(7)}); !r.ok || r.measured != "7" {
		t.Fatalf("soma dos brokers = %+v", r)
	}
	r := e.evaluate(Goal{Name: "delta", Type: goalMetric, Process: "consumidor:c1", Metric: receivedMetric,
		Window: dur(time.Second, 3*time.Second), Min: f(16)})
	if r.ok || r.measured != "15" {
		t.Fatalf("delta na janela = %+v, esperado 15 e reprovado", r)
	}
}

func TestReadSensorSummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.log")
	log := `time=x level=INFO msg="resumo parcial" medidas=1
time=y level=INFO msg="resumo final" sensores=3 medidas=120 confirmadas_buffer=118 pendentes=2 sem_inscritos=1 descartes_overflow=0 falhas_conexao=4
`
	if err := os.WriteFile(path, []byte(log), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	s, err := readSensorSummary(path)
	if err != nil || !s.found || s.measured != 120 || s.confirmed != 118 || s.pending != 2 || s.noSubscribers != 1 || s.connectFailures != 4 {
		t.Fatalf("resumo = %+v, %v", s, err)
	}
}

func TestDurationJSON(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"250ms"`)); err != nil || d.D() != 250*time.Millisecond {
		t.Fatalf("Duration = %v, %v", d, err)
	}
	if err := d.UnmarshalJSON([]byte(`5`)); err == nil {
		t.Fatalf("numero aceito como duracao")
	}
}

var (
	binOnce sync.Once
	binDir  string
	binErr  error
)

// TestReducedScenarios roda cada cenário reduzido com processos reais e exige
// todas as metas atingidas (task 017). Pulado com -short.
func TestReducedScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("cenarios reduzidos sobem processos reais; pulados com -short")
	}
	binOnce.Do(func() {
		binDir, binErr = os.MkdirTemp("", "maktuf-bin-")
		if binErr == nil {
			binErr = build(binDir)
		}
	})
	if binErr != nil {
		t.Fatalf("build: %v", binErr)
	}
	t.Cleanup(func() { _ = os.RemoveAll(binDir) })

	files, err := filepath.Glob("../cenarios/ci/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("cenarios reduzidos nao encontrados: %v", err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, file := range files {
		t.Run(strings.TrimSuffix(filepath.Base(file), ".json"), func(t *testing.T) {
			// Os logs ficam guardados se o cenário falhar, para diagnóstico.
			out, err := os.MkdirTemp("", "maktuf-cenario-")
			if err != nil {
				t.Fatalf("MkdirTemp: %v", err)
			}
			t.Cleanup(func() {
				if t.Failed() {
					t.Logf("resultados e logs guardados em %s", out)
					return
				}
				_ = os.RemoveAll(out)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			dir, results, err := run(ctx, file, out, binDir, quiet)
			if err != nil {
				t.Fatalf("run: %v", err)
			}
			for _, r := range results {
				if !r.ok {
					report, _ := os.ReadFile(filepath.Join(dir, "relatorio.md"))
					t.Errorf("meta %q: medido %s, alvo %s (%s)\n%s", r.goal.Name, r.measured, r.target, r.detail, report)
				}
			}
		})
	}
}
