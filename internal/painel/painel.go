// Package painel é uma aplicação de exemplo com interface web para
// demonstrar o Maktuf ao vivo: sensores e um consumidor comandados pela
// página, e os brokers, o Juiz e o vip apenas observados pelo /metrics.
//
// Os sensores e o consumidor são os mesmos do modo terminal
// (sensor.Manual e consumer.Manual), que usam só a biblioteca cliente; o que
// aparece na tela é o caminho real das mensagens.
package painel

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
)

//go:embed web/index.html
var webFS embed.FS

const (
	DefaultAddr       = "127.0.0.1:8080"
	DefaultConsumerID = "painel"
	metricsTimeout    = 700 * time.Millisecond
	recentEvents      = 200
)

// DefaultFilters são as inscrições iniciais do consumidor do painel.
var DefaultFilters = []string{"sensores/+/temperatura", "sensores/+/status"}

// BrokerEndpoint é um broker observado pelo /metrics.
type BrokerEndpoint struct {
	ID      string
	Metrics string
}

// Config configura o painel.
type Config struct {
	// Addr é o endereço HTTP da página.
	Addr string
	// ClientAddr é onde sensores e consumidor conectam (o vip).
	ClientAddr string
	DataDir    string
	ConsumerID string
	Filters    []string
	// Sensors são criados ao iniciar.
	Sensors []string
	// JudgeMetrics, VipMetrics e Brokers são os endereços /metrics
	// observados; vazios são ignorados.
	JudgeMetrics string
	VipMetrics   string
	Brokers      []BrokerEndpoint
	Reconnect    client.ReconnectOptions
	Logger       *slog.Logger
}

// uiEvent vai para a página pelo /api/eventos.
type uiEvent struct {
	Kind     string          `json:"tipo"`
	Time     time.Time       `json:"hora"`
	Delivery *consumer.Event `json:"entrega,omitempty"`
	Sensor   string          `json:"sensor,omitempty"`
	Text     string          `json:"texto,omitempty"`
}

// Server é o painel.
type Server struct {
	cfg Config
	log *slog.Logger

	mu      sync.Mutex
	sensors map[string]*sensor.Manual

	consumer *consumer.Manual

	hubMu  sync.Mutex
	subs   map[chan uiEvent]struct{}
	recent []uiEvent

	ctx    context.Context
	cancel context.CancelFunc
}

// New valida a configuração. Os sensores e o consumidor sobem em Start.
func New(cfg Config) (*Server, error) {
	if cfg.ClientAddr == "" {
		return nil, errors.New("painel: endereco dos clientes vazio")
	}
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.DataDir == "" {
		cfg.DataDir = filepath.Join("data", "painel")
	}
	if cfg.ConsumerID == "" {
		cfg.ConsumerID = DefaultConsumerID
	}
	if cfg.Filters == nil {
		cfg.Filters = DefaultFilters
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{
		cfg:     cfg,
		log:     cfg.Logger,
		sensors: map[string]*sensor.Manual{},
		subs:    map[chan uiEvent]struct{}{},
	}, nil
}

// Start conecta o consumidor, faz as inscrições iniciais e cria os sensores
// da configuração. O ctx vale para a vida do painel.
func (s *Server) Start(ctx context.Context) error {
	if err := os.MkdirAll(s.cfg.DataDir, 0o750); err != nil {
		return fmt.Errorf("diretorio dos buffers: %w", err)
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	c, err := consumer.NewManual(consumer.ManualConfig{
		Addr: s.cfg.ClientAddr, ClientID: s.cfg.ConsumerID, Reconnect: s.cfg.Reconnect, Logger: s.log,
		OnEvent: func(ev consumer.Event) {
			s.publish(uiEvent{Kind: "entrega", Time: ev.Time, Delivery: &ev})
		},
	})
	if err != nil {
		return err
	}
	if _, err := c.Start(s.ctx); err != nil {
		return fmt.Errorf("consumidor (o vip ou o broker em %s esta no ar?): %w", s.cfg.ClientAddr, err)
	}
	s.consumer = c
	for _, f := range s.cfg.Filters {
		s.action("consumidor", c.Subscribe(s.ctx, f))
	}
	for _, id := range s.cfg.Sensors {
		if _, err := s.addSensor(id); err != nil {
			return err
		}
	}
	return nil
}

// Close encerra sensores e consumidor.
func (s *Server) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.sensors {
		_ = m.Close()
	}
	if s.consumer != nil {
		return s.consumer.Close()
	}
	return nil
}

// ListenAndServe serve a página até ctx terminar.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	s.log.Info("painel disponivel", "url", "http://"+ln.Addr().String())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) addSensor(id string) (*sensor.Manual, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sensors[id]; ok {
		return nil, fmt.Errorf("sensor %q ja existe", id)
	}
	m, err := sensor.NewManual(sensor.ManualConfig{
		Addr: s.cfg.ClientAddr, SensorID: id,
		BufferPath: filepath.Join(s.cfg.DataDir, id+".buf"),
		Reconnect:  s.cfg.Reconnect, Logger: s.log,
	})
	if err != nil {
		return nil, err
	}
	s.sensors[id] = m
	go func() {
		text, err := m.Connect(s.ctx)
		if err == nil {
			s.action(id, text)
		}
	}()
	return m, nil
}

func (s *Server) sensor(id string) *sensor.Manual {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sensors[id]
}

func (s *Server) removeSensor(id string) bool {
	s.mu.Lock()
	m, ok := s.sensors[id]
	delete(s.sensors, id)
	s.mu.Unlock()
	if ok {
		_ = m.Close()
	}
	return ok
}

// action registra o texto de uma operação para a página.
func (s *Server) action(who, text string) {
	text = strings.TrimRight(text, "\n")
	if text == "" {
		return
	}
	s.publish(uiEvent{Kind: "acao", Time: time.Now(), Sensor: who, Text: text})
}

func (s *Server) publish(ev uiEvent) {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	s.recent = append(s.recent, ev)
	if len(s.recent) > recentEvents {
		s.recent = s.recent[len(s.recent)-recentEvents:]
	}
	for ch := range s.subs {
		select {
		case ch <- ev:
		default: // página lenta: perde o evento, a lista recente continua certa
		}
	}
}

func (s *Server) subscribe() (chan uiEvent, []uiEvent) {
	ch := make(chan uiEvent, 256)
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	s.subs[ch] = struct{}{}
	return ch, append([]uiEvent(nil), s.recent...)
}

func (s *Server) unsubscribe(ch chan uiEvent) {
	s.hubMu.Lock()
	defer s.hubMu.Unlock()
	delete(s.subs, ch)
}

// brokerView é o que a página mostra de cada broker.
type brokerView struct {
	ID          string  `json:"id"`
	Role        string  `json:"papel"`
	Reachable   bool    `json:"alcancavel"`
	Queue       float64 `json:"fila_assinantes"`
	Published   float64 `json:"publicadas"`
	FromJudge   bool    `json:"visto_pelo_juiz"`
	JudgeState  int     `json:"estado_juiz"`
	HasMetrics  bool    `json:"observado"`
	ReplicaSync bool    `json:"reserva_sincronizada"`
	// metricsDown: o /metrics do broker não respondeu. Vale mais que a
	// visão do Juiz, que só percebe a queda depois do prazo de falha.
	metricsDown bool
}

type infraView struct {
	Brokers        []brokerView `json:"brokers"`
	JudgeReachable bool         `json:"juiz_alcancavel"`
	JudgeKnown     bool         `json:"juiz_configurado"`
	VipReachable   bool         `json:"vip_alcancavel"`
	VipAddr        string       `json:"vip"`
}

type stateView struct {
	Sensors  []sensor.ManualState `json:"sensores"`
	Consumer consumer.ManualState `json:"consumidor"`
	Infra    infraView            `json:"infra"`
}

func (s *Server) state() stateView {
	s.mu.Lock()
	ids := make([]string, 0, len(s.sensors))
	for id := range s.sensors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	states := make([]sensor.ManualState, 0, len(ids))
	for _, id := range ids {
		states = append(states, s.sensors[id].State())
	}
	s.mu.Unlock()
	return stateView{Sensors: states, Consumer: s.consumer.State(), Infra: s.infra()}
}

// infra lê o /metrics do Juiz e dos brokers e testa o vip.
func (s *Server) infra() infraView {
	v := infraView{VipAddr: s.cfg.ClientAddr, JudgeKnown: s.cfg.JudgeMetrics != ""}
	byID := map[string]*brokerView{}
	order := []string{}
	get := func(id string) *brokerView {
		if b, ok := byID[id]; ok {
			return b
		}
		b := &brokerView{ID: id}
		byID[id] = b
		order = append(order, id)
		return b
	}
	for _, b := range s.cfg.Brokers {
		get(b.ID)
	}
	if s.cfg.JudgeMetrics != "" {
		if fams, err := scrape(s.cfg.JudgeMetrics); err == nil {
			v.JudgeReachable = true
			for _, f := range fams {
				if f.Name != "judge_broker_state" {
					continue
				}
				for _, smp := range f.Samples {
					for _, l := range smp.Labels {
						if l.Name == "broker_id" {
							b := get(l.Value)
							b.FromJudge = true
							b.JudgeState = int(smp.Value)
						}
					}
				}
			}
		}
	}
	for _, ep := range s.cfg.Brokers {
		b := get(ep.ID)
		b.HasMetrics = true
		fams, err := scrape(ep.Metrics)
		if err != nil {
			b.metricsDown = true
			continue
		}
		b.Reachable = true
		b.JudgeState = int(sum(fams, "broker_state", nil))
		b.Queue = sum(fams, "broker_queue_depth", map[string]string{"queue": "subscriber"})
		b.Published = sum(fams, "messages_published_total", nil)
		b.ReplicaSync = sum(fams, "replica_synced", nil) == 1
	}
	for _, id := range order {
		b := byID[id]
		if b.FromJudge && b.JudgeState != 0 && !b.metricsDown {
			b.Reachable = true
		}
		b.Role = roleName(b.JudgeState, b.Reachable)
		v.Brokers = append(v.Brokers, *b)
	}
	if conn, err := net.DialTimeout("tcp", s.cfg.ClientAddr, metricsTimeout); err == nil {
		_ = conn.Close()
		v.VipReachable = true
	}
	return v
}

func roleName(state int, reachable bool) string {
	if !reachable {
		return "fora do ar"
	}
	switch protocol.BrokerState(state) {
	case protocol.BrokerStatePrimary:
		return "primario"
	case protocol.BrokerStateBackupSynced:
		return "reserva sincronizada"
	case protocol.BrokerStateBackupSyncing:
		return "reserva sincronizando"
	case protocol.BrokerStateFenced:
		return "isolado"
	default:
		return "no ar"
	}
}

func scrape(addr string) ([]metrics.Family, error) {
	c := http.Client{Timeout: metricsTimeout}
	resp, err := c.Get("http://" + addr + metrics.Path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return metrics.ParseText(resp.Body)
}

func sum(fams []metrics.Family, name string, labels map[string]string) float64 {
	var total float64
	for _, f := range fams {
		if f.Name != name {
			continue
		}
	samples:
		for _, smp := range f.Samples {
			if smp.Suffix != "" {
				continue
			}
			for k, want := range labels {
				found := false
				for _, l := range smp.Labels {
					if l.Name == k && l.Value == want {
						found = true
					}
				}
				if !found {
					continue samples
				}
			}
			total += smp.Value
		}
	}
	return total
}
