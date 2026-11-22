package painel_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/painel"
)

const waitTimeout = 5 * time.Second

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func startBroker(t *testing.T) (addr, metricsAddr string) {
	t.Helper()
	srv := broker.NewServer(broker.Config{Addr: "127.0.0.1:0", Logger: quiet})
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	m := httptest.NewServer(srv.Metrics().Handler())
	t.Cleanup(m.Close)
	return srv.Addr().String(), strings.TrimPrefix(m.URL, "http://")
}

// fakeJudge responde o /metrics do Juiz com b1 primário.
func fakeJudge(t *testing.T) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "# TYPE judge_broker_state gauge\njudge_broker_state{broker_id=\"b1\"} 1\n")
	}))
	t.Cleanup(s.Close)
	return strings.TrimPrefix(s.URL, "http://")
}

type harness struct {
	t   *testing.T
	url string
}

func startPainel(t *testing.T) *harness {
	t.Helper()
	addr, brokerMetrics := startBroker(t)
	p, err := painel.New(painel.Config{
		ClientAddr:   addr,
		DataDir:      t.TempDir(),
		JudgeMetrics: fakeJudge(t),
		Brokers:      []painel.BrokerEndpoint{{ID: "b1", Metrics: brokerMetrics}},
		Logger:       quiet,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	web := httptest.NewServer(p.Handler())
	t.Cleanup(func() {
		cancel()
		web.Close()
		_ = p.Close()
	})
	return &harness{t: t, url: web.URL}
}

func (h *harness) call(method, path string, body any) (int, map[string]any) {
	h.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, h.url+path, &buf)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

type state struct {
	Sensors []struct {
		ID         string `json:"id"`
		Connection string `json:"conexao"`
		LastSeq    uint64 `json:"ultimo_seq"`
	} `json:"sensores"`
	Consumer struct {
		Received      uint64   `json:"recebidas"`
		Subscriptions []string `json:"inscricoes"`
	} `json:"consumidor"`
	Infra struct {
		Brokers []struct {
			ID   string `json:"id"`
			Role string `json:"papel"`
		} `json:"brokers"`
		JudgeReachable bool `json:"juiz_alcancavel"`
		VipReachable   bool `json:"vip_alcancavel"`
	} `json:"infra"`
}

func (h *harness) state() state {
	h.t.Helper()
	resp, err := http.Get(h.url + "/api/estado")
	if err != nil {
		h.t.Fatalf("estado: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var st state
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		h.t.Fatalf("decode estado: %v", err)
	}
	return st
}

func (h *harness) waitState(what string, cond func(state) bool) state {
	h.t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		st := h.state()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timeout aguardando %s: %+v", what, st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPainelSendsReadingsAndStreamsDeliveries(t *testing.T) {
	h := startPainel(t)

	// A página carrega.
	resp, err := http.Get(h.url + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	page, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(page), "Maktuf") {
		t.Fatalf("pagina sem o titulo")
	}

	// Fluxo ao vivo, aberto antes do envio.
	stream, err := http.Get(h.url + "/api/eventos")
	if err != nil {
		t.Fatalf("eventos: %v", err)
	}
	defer func() { _ = stream.Body.Close() }()
	delivered := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stream.Body)
		for sc.Scan() {
			if line := sc.Text(); strings.Contains(line, `"tipo":"leitura"`) && strings.Contains(line, `"sensor":"sala-01"`) {
				delivered <- line
				return
			}
		}
	}()

	if code, body := h.call("POST", "/api/sensores", map[string]string{"id": "sala-01"}); code != http.StatusCreated {
		t.Fatalf("criar sensor: %d %v", code, body)
	}
	h.waitState("sensor conectado", func(st state) bool {
		return len(st.Sensors) == 1 && st.Sensors[0].Connection == "conectado"
	})

	code, body := h.call("POST", "/api/sensores/sala-01/enviar", map[string]any{"temperatura": 23.5})
	if code != http.StatusOK || !strings.Contains(body["texto"].(string), "PERSISTED") {
		t.Fatalf("enviar: %d %v", code, body)
	}
	select {
	case line := <-delivered:
		if !strings.Contains(line, `"seq":1`) || !strings.Contains(line, `"temperatura":23.5`) {
			t.Fatalf("entrega = %s", line)
		}
	case <-time.After(waitTimeout):
		t.Fatalf("a entrega nao chegou pelo /api/eventos")
	}

	st := h.waitState("consumidor receber", func(st state) bool { return st.Consumer.Received == 1 })
	if len(st.Infra.Brokers) != 1 || st.Infra.Brokers[0].Role != "primario" || !st.Infra.JudgeReachable || !st.Infra.VipReachable {
		t.Fatalf("infra = %+v", st.Infra)
	}

	// Sem a inscrição em leituras, o sensor volta a ver NO_SUBSCRIBERS.
	if code, body := h.call("POST", "/api/consumidor/cancelar", map[string]string{"filtro": "sensores/+/temperatura"}); code != http.StatusOK ||
		!strings.Contains(body["texto"].(string), "cancelada") {
		t.Fatalf("cancelar: %d %v", code, body)
	}
	if _, body := h.call("POST", "/api/sensores/sala-01/enviar", map[string]any{"temperatura": 24}); !strings.Contains(body["texto"].(string), "NO_SUBSCRIBERS") {
		t.Fatalf("enviar depois de cancelar: %v", body)
	}
}

func TestPainelSensorActionsAndErrors(t *testing.T) {
	h := startPainel(t)
	h.call("POST", "/api/sensores", map[string]string{"id": "lab-02"})
	h.waitState("sensor conectado", func(st state) bool {
		return len(st.Sensors) == 1 && st.Sensors[0].Connection == "conectado"
	})

	if code, _ := h.call("POST", "/api/sensores", map[string]string{"id": "lab-02"}); code != http.StatusBadRequest {
		t.Fatalf("sensor duplicado aceito: %d", code)
	}
	if code, _ := h.call("POST", "/api/sensores", map[string]string{"id": "com/barra"}); code != http.StatusBadRequest {
		t.Fatalf("id invalido aceito: %d", code)
	}
	if code, _ := h.call("POST", "/api/sensores/nao-existe/enviar", map[string]any{"temperatura": 1}); code != http.StatusNotFound {
		t.Fatalf("sensor inexistente: %d", code)
	}
	if code, _ := h.call("POST", "/api/sensores/lab-02/enviar", map[string]any{}); code != http.StatusBadRequest {
		t.Fatalf("enviar sem temperatura: %d", code)
	}

	if code, _ := h.call("POST", "/api/sensores/lab-02/rajada", map[string]any{"quantidade": 20, "temperatura": 21}); code != http.StatusAccepted {
		t.Fatalf("rajada: %d", code)
	}
	h.waitState("rajada entregue", func(st state) bool { return st.Consumer.Received == 20 })

	if _, body := h.call("POST", "/api/sensores/lab-02/desconectar", nil); !strings.Contains(body["texto"].(string), "conexao derrubada") {
		t.Fatalf("desconectar: %v", body)
	}
	if _, body := h.call("POST", "/api/sensores/lab-02/enviar", map[string]any{"temperatura": 30}); !strings.Contains(body["texto"].(string), "guardada no buffer") {
		t.Fatalf("enviar desconectado: %v", body)
	}
	if _, body := h.call("POST", "/api/sensores/lab-02/conectar", nil); !strings.Contains(body["texto"].(string), "reenviando 1") {
		t.Fatalf("conectar: %v", body)
	}
	h.waitState("reenvio entregue", func(st state) bool { return st.Consumer.Received == 21 })

	if _, body := h.call("POST", "/api/sensores/lab-02/dormir", map[string]any{"segundos": 2}); !strings.Contains(body["texto"].(string), "SLEEP(2s) enviado") {
		t.Fatalf("dormir: %v", body)
	}
	h.waitState("sensor dormindo", func(st state) bool { return st.Sensors[0].Connection == "dormindo" })

	if code, _ := h.call("DELETE", "/api/sensores/lab-02", nil); code != http.StatusOK {
		t.Fatalf("remover: %d", code)
	}
	h.waitState("sensor removido", func(st state) bool { return len(st.Sensors) == 0 })
}
