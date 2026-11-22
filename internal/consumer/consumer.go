// Package consumer implementa o serviço consumidor de exemplo do Maktuf
// (RF-60): inscreve-se nos tópicos de leitura e de status, deduplica por
// sensor_id + seq, detecta lacunas de seq e mede latência, usando apenas as
// operações da biblioteca cliente (RF-51).
package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

const (
	DefaultClientID      = "consumidor-1"
	DefaultRetryInterval = time.Second
)

// DefaultFilters são as inscrições do consumidor de exemplo.
var DefaultFilters = []string{"sensores/+/temperatura", "sensores/+/status"}

var ErrInvalidConfig = errors.New("consumer: configuracao invalida")

// Config descreve o consumidor. Campos zerados recebem os defaults.
type Config struct {
	Addr     string
	ClientID string
	Filters  []string
	// DedupWindow é quantas chaves sensor_id + seq a deduplicação lembra.
	DedupWindow    int
	LatencySamples int
	// RetryInterval é a espera entre tentativas da conexão inicial. Depois
	// de conectado, a reconexão é da biblioteca (Reconnect).
	RetryInterval time.Duration
	// ProcessDelay simula um consumidor lento: espera por mensagem antes de
	// confirmar a próxima (RF-36, CT-10). Zero não espera.
	ProcessDelay time.Duration
	// Display, quando presente, recebe uma linha legível por leitura,
	// transição de estado, repetida e lacuna (modo de apresentação).
	Display   io.Writer
	Reconnect client.ReconnectOptions
	// Metrics recebe as métricas do consumidor. Nil cria um registro
	// próprio, acessível por Consumer.Metrics.
	Metrics *metrics.Registry
	Logger  *slog.Logger
}

// Consumer recebe as leituras e as passa pelo Tracker.
type Consumer struct {
	cfg     Config
	log     *slog.Logger
	tracker *Tracker
	metrics *consumerMetrics
	ready   chan struct{}
}

// New valida a configuração e prepara o consumidor.
func New(cfg Config) (*Consumer, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("%w: endereco do broker vazio", ErrInvalidConfig)
	}
	if cfg.ClientID == "" {
		cfg.ClientID = DefaultClientID
	}
	if len(cfg.Filters) == 0 {
		cfg.Filters = DefaultFilters
	}
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = DefaultRetryInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	return &Consumer{
		cfg:     cfg,
		log:     cfg.Logger.With("client_id", cfg.ClientID),
		tracker: NewTracker(cfg.DedupWindow, cfg.LatencySamples),
		metrics: newConsumerMetrics(cfg.Metrics),
		ready:   make(chan struct{}),
	}, nil
}

// Metrics devolve o registro com as métricas do consumidor.
func (c *Consumer) Metrics() *metrics.Registry { return c.cfg.Metrics }

// Ready é fechado quando todas as inscrições foram confirmadas pelo broker.
func (c *Consumer) Ready() <-chan struct{} { return c.ready }

// Stats devolve as métricas acumuladas.
func (c *Consumer) Stats() Stats { return c.tracker.Stats() }

// Run conecta, inscreve-se e processa mensagens até o contexto terminar,
// que é o encerramento normal e devolve nil.
func (c *Consumer) Run(ctx context.Context) error {
	cl, ok := c.connect(ctx)
	if !ok {
		return nil
	}
	defer func() { _ = cl.Close() }()

	for _, filter := range c.cfg.Filters {
		if err := cl.Subscribe(ctx, filter); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: inscrever em %q: %w", filter, err)
		}
		c.log.Info("inscrito", "filtro", filter)
	}
	close(c.ready)

	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-cl.Messages():
			if !ok {
				return fmt.Errorf("consumer: cliente encerrado: %w", cl.Err())
			}
			c.handle(m)
		}
	}
}

// connect insiste na conexão inicial até conseguir; a biblioteca só reconecta
// sozinha depois da primeira conexão. Devolve false se o contexto terminar.
func (c *Consumer) connect(ctx context.Context) (*client.Client, bool) {
	for {
		cl, err := client.ConnectWithOptions(ctx, c.cfg.Addr, client.RoleConsumer, c.cfg.ClientID, client.Options{
			Reconnect: c.cfg.Reconnect,
		})
		if err == nil {
			c.log.Info("conectado ao broker", "addr", c.cfg.Addr)
			return cl, true
		}
		c.log.Warn("conexao com o broker falhou; nova tentativa", "error", err, "em", c.cfg.RetryInterval)

		t := time.NewTimer(c.cfg.RetryInterval)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, false
		}
	}
}

func (c *Consumer) handle(m client.Message) {
	obs := c.tracker.Observe(m, time.Now())
	c.metrics.observe(obs, m)
	if c.cfg.ProcessDelay > 0 {
		time.Sleep(c.cfg.ProcessDelay)
	}
	if c.cfg.Display != nil {
		c.display(m, obs)
	}
	log := c.log.With("sensor_id", m.SensorID, "topic", m.Topic, "seq", m.Seq)

	switch obs.Outcome {
	case OutcomeStatus:
		log.Info("transicao de estado do sensor", "estado", m.Status.String())
	case OutcomeDuplicate, OutcomeOld:
		log.Debug("leitura descartada", "motivo", obs.Outcome.String())
	case OutcomeAccepted:
		if obs.Missing > 0 {
			cause := "sem marca (overflow do sensor ou perda)"
			if obs.Marked {
				cause = "retencao do assinante (GAP_BEFORE)"
			}
			log.Warn("lacuna de sequencia", "faltando", obs.Missing, "causa", cause)
		} else if obs.Marked {
			log.Warn("lacuna sinalizada pelo broker sem referencia anterior")
		}
		log.Debug("leitura recebida",
			"temperatura", m.Temperature,
			"timestamp", m.Timestamp.Format(time.RFC3339Nano),
			"latencia", obs.Latency)
	}
}

// display escreve a mensagem em uma linha legível (modo de apresentação).
func (c *Consumer) display(m client.Message, obs Observation) {
	writeDisplay(c.cfg.Display, m, obs)
}

// writeDisplay escreve uma linha legível por leitura, transição de estado,
// repetida ou lacuna. Usada pelo -mostrar e pelo consumidor manual.
func writeDisplay(w io.Writer, m client.Message, obs Observation) {
	now := time.Now().Format("15:04:05")
	say := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	switch obs.Outcome {
	case OutcomeStatus:
		say("%s  %-14s agora esta %s\n", now, m.SensorID, strings.ToUpper(m.Status.String()))
	case OutcomeDuplicate:
		say("%s  %-14s seq %-6d repetida (descartada pela deduplicacao)\n", now, m.SensorID, m.Seq)
	case OutcomeOld:
		say("%s  %-14s seq %-6d antiga (ja processada; descartada)\n", now, m.SensorID, m.Seq)
	case OutcomeAccepted:
		switch {
		case obs.Missing > 0 && obs.Marked:
			say("%s  %-14s ATENCAO: %d leitura(s) perdida(s) por retencao da fila (GAP_BEFORE)\n", now, m.SensorID, obs.Missing)
		case obs.Missing > 0:
			say("%s  %-14s ATENCAO: %d leitura(s) faltando antes desta (buffer do sensor cheio ou perda)\n", now, m.SensorID, obs.Missing)
		case obs.Marked:
			say("%s  %-14s ATENCAO: o broker avisou perda por retencao antes desta leitura\n", now, m.SensorID)
		}
		say("%s  %-14s seq %-6d %7.2f C   medida ha %s\n",
			now, m.SensorID, m.Seq, m.Temperature, formatAge(obs.Latency))
	}
}

// formatAge mostra a idade da leitura em ms, s ou min, conforme o tamanho.
func formatAge(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1f s", d.Seconds())
	default:
		return d.Round(time.Second).String()
	}
}
