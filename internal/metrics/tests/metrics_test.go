package metrics_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

func expose(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var buf bytes.Buffer
	if err := metrics.WriteText(&buf, r.Gather()); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	return buf.String()
}

func expectPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s deveria entrar em panico", what)
		}
	}()
	fn()
}

func TestCounterWithAndWithoutLabels(t *testing.T) {
	r := metrics.NewRegistry()
	plain := r.NewCounter("eventos_total", "Eventos.")
	labeled := r.NewCounter("leituras_total", "Leituras.", "sensor_id")

	plain.Inc()
	plain.Add(4)
	labeled.Inc("a")
	labeled.Inc("a")
	labeled.Inc("b")

	if got := plain.Value(); got != 5 {
		t.Fatalf("plain = %d, esperado 5", got)
	}
	if got := labeled.Value("a"); got != 2 {
		t.Fatalf("a = %d, esperado 2", got)
	}
	if got := labeled.Value("b"); got != 1 {
		t.Fatalf("b = %d, esperado 1", got)
	}
}

func TestCounterIsConcurrentSafe(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("concorrente_total", "Concorrente.", "id")

	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				c.Inc("x")
			}
		}()
	}
	wg.Wait()
	if got := c.Value("x"); got != 100000 {
		t.Fatalf("valor = %d, esperado 100000", got)
	}
}

func TestRegistrationErrorsPanic(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("ok_total", "Ok.", "sensor_id")
	r.NewHistogram("lat_seconds", "Lat.", nil)

	expectPanic(t, "quantidade de rotulos errada", func() { c.Inc() })
	expectPanic(t, "nome invalido", func() { r.NewCounter("9invalido", "x") })
	expectPanic(t, "rotulo invalido", func() { r.NewCounter("rot_total", "x", "com-hifen") })
	expectPanic(t, "nome duplicado", func() { r.NewGauge("ok_total", "x") })
	expectPanic(t, "nome reservado pelo _quantile", func() { r.NewGauge("lat_seconds_quantile", "x") })
	expectPanic(t, "rotulo le em histograma", func() { r.NewHistogram("h_seconds", "x", nil, "le") })
	expectPanic(t, "buckets fora de ordem", func() { r.NewHistogram("h2_seconds", "x", []float64{1, 1}) })
}

func TestGaugeSetAndAdd(t *testing.T) {
	r := metrics.NewRegistry()
	g := r.NewGauge("fila", "Fila.", "id")

	g.Set(3, "a")
	g.Add(-1.5, "a")
	if got := g.Value("a"); got != 1.5 {
		t.Fatalf("valor = %v, esperado 1.5", got)
	}
}

func TestHistogramQuantileAndCount(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.NewHistogram("lat_seconds", "Lat.", []float64{1, 2, 4})

	if q := h.Quantile(0.5); !math.IsNaN(q) {
		t.Fatalf("sem dados = %v, esperado NaN", q)
	}

	// 10 observações em (1, 2]: a mediana cai no meio do bucket.
	for i := 0; i < 10; i++ {
		h.Observe(1.5)
	}
	if got := h.Count(); got != 10 {
		t.Fatalf("count = %d", got)
	}
	if q := h.Quantile(0.5); q != 1.5 {
		t.Fatalf("p50 = %v, esperado 1.5 (interpolacao em (1,2])", q)
	}

	h.Observe(100) // cai no +Inf
	if q := h.Quantile(0.99); q != 4 {
		t.Fatalf("p99 no +Inf = %v, esperado o maior limite finito 4", q)
	}
}

func TestWriteTextFormat(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("pub_total", "Publicacoes\nrecebidas.", "topic")
	h := r.NewHistogram("ack_seconds", "Latencia do ACK.", []float64{0.5, 1})

	c.Inc(`sensores/"a"\x`)
	h.Observe(0.25)
	h.Observe(0.75)

	want := `# HELP ack_seconds Latencia do ACK.
# TYPE ack_seconds histogram
ack_seconds_bucket{le="0.5"} 1
ack_seconds_bucket{le="1"} 2
ack_seconds_bucket{le="+Inf"} 2
ack_seconds_sum 1
ack_seconds_count 2
# HELP ack_seconds_quantile Percentis de ack_seconds estimados pelos buckets.
# TYPE ack_seconds_quantile gauge
ack_seconds_quantile{quantile="0.5"} 0.5
ack_seconds_quantile{quantile="0.95"} 0.95
ack_seconds_quantile{quantile="0.99"} 0.99
# HELP pub_total Publicacoes\nrecebidas.
# TYPE pub_total counter
pub_total{topic="sensores/\"a\"\\x"} 1
`
	if got := expose(t, r); got != want {
		t.Fatalf("exposicao diferente.\n--- obtido\n%s--- esperado\n%s", got, want)
	}
}

func TestFuncMetricsAreCollectedOnEveryGather(t *testing.T) {
	r := metrics.NewRegistry()
	depth := 1
	r.NewGaugeFunc("profundidade", "Profundidade.", []string{"id"}, func() []metrics.Sample {
		return []metrics.Sample{
			{Labels: []metrics.Label{{Name: "id", Value: "b"}}, Value: float64(depth)},
			{Labels: []metrics.Label{{Name: "id", Value: "a"}}, Value: float64(depth * 10)},
			// Rótulos errados são ignorados.
			{Labels: []metrics.Label{{Name: "outro", Value: "x"}}, Value: 99},
		}
	})

	if got := expose(t, r); !strings.Contains(got, "profundidade{id=\"a\"} 10\nprofundidade{id=\"b\"} 1\n") || strings.Contains(got, "99") {
		t.Fatalf("primeira leitura:\n%s", got)
	}
	depth = 2
	if got := expose(t, r); !strings.Contains(got, `profundidade{id="b"} 2`) {
		t.Fatalf("segunda leitura nao recalculou:\n%s", got)
	}
}

func TestHandler(t *testing.T) {
	r := metrics.NewRegistry()
	r.NewCounter("x_total", "X.").Inc()
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != metrics.ContentType {
		t.Fatalf("status %d, content-type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(body), "x_total 1") {
		t.Fatalf("corpo:\n%s", body)
	}

	resp, err = http.Post(srv.URL, "text/plain", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, esperado 405", resp.StatusCode)
	}
}

func TestServeStopsWithContext(t *testing.T) {
	r := metrics.NewRegistry()
	r.NewCounter("y_total", "Y.").Inc()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- metrics.Serve(ctx, ln, r, nil) }()

	resp, err := http.Get("http://" + ln.Addr().String() + metrics.Path)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Serve nao encerrou com o contexto")
	}
}

// Task 017: o runner lê o /metrics de volta; ParseText precisa reconstruir
// exatamente o que WriteText escreveu.
func TestParseTextRoundTrip(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("pub_total", "Publicacoes\ncom \\ barra.", "topic", "sensor_id")
	g := r.NewGauge("fila", "Fila.")
	h := r.NewHistogram("lat_seconds", "Lat.", []float64{0.5, 1}, "group")
	c.Inc(`sensores/"a"`, "a")
	c.Add(3, "x,y", "b")
	g.Set(-2.5)
	h.Observe(0.25, "sensor")
	h.Observe(2, "sensor")

	want := r.Gather()
	got, err := metrics.ParseText(strings.NewReader(expose(t, r)))
	if err != nil {
		t.Fatalf("ParseText: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("familias = %d, esperado %d", len(got), len(want))
	}
	for i := range want {
		w, g := want[i], got[i]
		if w.Name != g.Name || w.Help != g.Help || w.Type != g.Type || len(w.Samples) != len(g.Samples) {
			t.Fatalf("familia %d = %+v, esperado %+v", i, g, w)
		}
		for j := range w.Samples {
			ws, gs := w.Samples[j], g.Samples[j]
			if ws.Suffix != gs.Suffix || ws.Value != gs.Value || len(ws.Labels) != len(gs.Labels) {
				t.Fatalf("%s amostra %d = %+v, esperado %+v", w.Name, j, gs, ws)
			}
			for k := range ws.Labels {
				if ws.Labels[k] != gs.Labels[k] {
					t.Fatalf("%s rotulo = %+v, esperado %+v", w.Name, gs.Labels[k], ws.Labels[k])
				}
			}
		}
	}
}

func TestEstimateQuantileFromCumulativeBuckets(t *testing.T) {
	bounds := []float64{1, 2, 4}
	// 10 observações em (1,2] e nada nos demais.
	cumulative := []uint64{0, 10, 10, 10}
	if q := metrics.EstimateQuantile(0.5, bounds, cumulative); q != 1.5 {
		t.Fatalf("p50 = %v, esperado 1.5", q)
	}
	if q := metrics.EstimateQuantile(0.5, bounds, []uint64{0, 0, 0, 0}); !math.IsNaN(q) {
		t.Fatalf("sem dados = %v, esperado NaN", q)
	}
}
