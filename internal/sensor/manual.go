package sensor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

// ManualConfig configura um sensor controlado por comandos de texto, feito
// para demonstrar o sistema ao vivo.
type ManualConfig struct {
	Addr       string
	SensorID   string
	BufferPath string
	BufferSize int
	// PublishTimeout limita a espera pelo PUBACK de cada leitura.
	PublishTimeout time.Duration
	Reconnect      client.ReconnectOptions
	Logger         *slog.Logger
}

// Manual é um único sensor dirigido por comandos (Run). Usa só a biblioteca
// cliente e o buffer durável, como um sensor real.
type Manual struct {
	cfg    ManualConfig
	topic  string
	buffer *storage.FileBuffer
	client *client.Client
	out    io.Writer
	// offline é a queda simulada por "desconectar": as leituras vão para o
	// buffer até um "conectar" explícito. Depois de "dormir", não: a próxima
	// leitura acorda o sensor.
	offline bool
}

const manualHelp = `comandos:
  <temperatura>         publica uma leitura (ex.: 23.5)
  rajada <n> [temp]     grava n leituras de uma vez e envia
  dormir <duracao>      envia SLEEP e desconecta (ex.: dormir 30s); a proxima leitura reconecta
  desconectar           derruba a conexao sem avisar (falha); as leituras ficam no buffer
  conectar              reconecta e reenvia o que estiver pendente
  status                mostra conexao, ultimo seq e buffer
  pendentes             lista as leituras ainda no buffer
  ajuda                 mostra esta ajuda
  sair                  encerra
`

// NewManual abre o buffer do sensor. A conexão é feita no primeiro uso.
func NewManual(cfg ManualConfig) (*Manual, error) {
	switch {
	case cfg.Addr == "":
		return nil, fmt.Errorf("%w: endereco do broker vazio", ErrInvalidConfig)
	case cfg.SensorID == "" || strings.Contains(cfg.SensorID, "/"):
		return nil, fmt.Errorf("%w: id do sensor %q", ErrInvalidConfig, cfg.SensorID)
	case cfg.BufferPath == "":
		return nil, fmt.Errorf("%w: arquivo do buffer vazio", ErrInvalidConfig)
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	buffer, err := storage.OpenSensorBuffer(cfg.BufferPath, storage.BufferOptions{
		MaxReadings: cfg.BufferSize,
		SensorID:    cfg.SensorID,
		Logger:      cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	return &Manual{cfg: cfg, topic: "sensores/" + cfg.SensorID + "/temperatura", buffer: buffer}, nil
}

// Close desconecta e fecha o buffer.
func (m *Manual) Close() error {
	m.disconnect()
	return m.buffer.Close()
}

// Run lê um comando por linha de in e escreve as respostas em out, até
// "sair", fim da entrada ou ctx terminar.
func (m *Manual) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	m.out = out
	m.printf("sensor %s -> %s (topico %s)\n", m.cfg.SensorID, m.cfg.Addr, m.topic)
	if st := m.buffer.Stats(); st.Pending > 0 || st.LastSeq > 0 {
		m.printf("buffer reaberto: ultimo seq %d, %d pendentes\n", st.LastSeq, st.Pending)
	}
	m.connect(ctx)
	m.printf("digite 'ajuda' para ver os comandos\n")

	// A leitura fica numa goroutine para o Ctrl+C (ctx) encerrar mesmo com o
	// terminal parado esperando uma linha.
	lines := make(chan string)
	readErr := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(in)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
		readErr <- sc.Err()
	}()
	for {
		m.printf("%s> ", m.cfg.SensorID)
		select {
		case <-ctx.Done():
			m.printf("\n")
			return nil
		case err := <-readErr:
			m.printf("\n")
			return err
		case line := <-lines:
			if !m.handle(ctx, strings.Fields(line)) {
				return nil
			}
		}
	}
}

func (m *Manual) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(m.out, format, args...)
}

// handle executa um comando; devolve false para encerrar.
func (m *Manual) handle(ctx context.Context, args []string) bool {
	if len(args) == 0 {
		return true
	}
	switch cmd := strings.ToLower(args[0]); cmd {
	case "sair", "exit", "quit":
		return false
	case "ajuda", "help", "?":
		m.printf("%s", manualHelp)
	case "status":
		m.status()
	case "pendentes":
		m.pending()
	case "conectar":
		m.offline = false
		if m.client != nil {
			if m.client.Connected() {
				m.printf("  ja conectado\n")
			} else {
				m.printf("  a conexao caiu e a biblioteca esta tentando reconectar a %s sozinha, ainda sem sucesso.\n", m.cfg.Addr)
				m.printf("  Se esse endereco for um broker que virou reserva, saia e use o vip (-addr 127.0.0.1:7070)\n")
			}
			return true
		}
		m.connect(ctx)
	case "desconectar":
		if m.client == nil {
			m.printf("  ja desconectado\n")
			return true
		}
		m.disconnect()
		m.offline = true
		m.printf("  conexao derrubada sem SLEEP; para o broker e uma falha. Leituras vao para o buffer ate 'conectar'\n")
	case "dormir":
		m.sleep(ctx, args[1:])
	case "rajada":
		m.burst(ctx, args[1:])
	default:
		temperature, err := strconv.ParseFloat(strings.ReplaceAll(cmd, ",", "."), 32)
		if err != nil {
			m.printf("  comando desconhecido %q; digite 'ajuda'\n", args[0])
			return true
		}
		m.publish(ctx, float32(temperature))
	}
	return true
}

func (m *Manual) connect(ctx context.Context) {
	dialCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	c, err := client.ConnectWithOptions(dialCtx, m.cfg.Addr, client.RoleSensor, m.cfg.SensorID, client.Options{
		Reconnect: m.cfg.Reconnect,
		Sensor:    &client.SensorOptions{Buffer: m.buffer, Topic: m.topic},
	})
	if err != nil {
		m.printf("  sem conexao com %s (%v); as leituras ficam no buffer\n", m.cfg.Addr, err)
		return
	}
	m.client = c
	pending := m.buffer.Stats().Pending
	m.printf("  conectado (broker publica 'online' no topico de status)\n")
	if pending > 0 {
		m.printf("  reenviando %d leituras pendentes, em ordem de seq\n", pending)
	}
}

func (m *Manual) disconnect() {
	if m.client != nil {
		_ = m.client.Close()
		m.client = nil
	}
}

func (m *Manual) publish(ctx context.Context, temperature float32) {
	if m.client == nil && !m.offline {
		m.connect(ctx)
	}
	if m.client == nil {
		r, err := m.buffer.Append(time.Now(), temperature)
		if err != nil {
			m.printf("  erro ao gravar no buffer: %v\n", err)
			return
		}
		m.printf("  seq=%d guardada no buffer (desconectado); pendentes: %d\n", r.Seq, m.buffer.Stats().Pending)
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	r, status, err := m.client.PublishBuffered(pubCtx, time.Now(), temperature)
	switch {
	case err != nil && r.Seq == 0:
		m.printf("  erro ao gravar no buffer: %v\n", err)
	case err != nil:
		m.printf("  seq=%d sem PUBACK por enquanto (%v); fica no buffer e sera reenviada. Pendentes: %d\n",
			r.Seq, shortErr(err), m.buffer.Stats().Pending)
		if !m.client.Connected() {
			m.printf("  (sem conexao: reconectando a %s)\n", m.cfg.Addr)
		}
	default:
		m.printf("  seq=%d %s\n", r.Seq, describeStatus(status))
	}
}

func describeStatus(s client.Status) string {
	switch s {
	case client.StatusPersisted:
		return "PERSISTED: gravada no broker (e na reserva, se sincronizada)"
	case client.StatusNoSubscribers:
		return "NO_SUBSCRIBERS: ninguem inscrito; o broker descartou e o sensor tirou do buffer"
	default:
		return s.String()
	}
}

func shortErr(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "tempo esgotado"
	case errors.Is(err, client.ErrDisconnected):
		return "conexao caiu"
	default:
		return err.Error()
	}
}

func (m *Manual) sleep(ctx context.Context, args []string) {
	if len(args) != 1 {
		m.printf("  uso: dormir <duracao>, por exemplo: dormir 30s\n")
		return
	}
	d, err := time.ParseDuration(args[0])
	if err != nil || d <= 0 {
		m.printf("  duracao invalida %q (use 30s, 2m...)\n", args[0])
		return
	}
	if m.offline {
		m.printf("  desconectado: use 'conectar' antes de dormir\n")
		return
	}
	if m.client == nil {
		m.connect(ctx)
	}
	if m.client == nil {
		m.printf("  sem conexao: nao da para avisar o broker\n")
		return
	}
	sleepCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	if err := m.client.Sleep(sleepCtx, d); err != nil {
		m.printf("  SLEEP nao enviado: %v\n", err)
		return
	}
	m.disconnect()
	m.printf("  SLEEP(%s) enviado e conexao fechada: o broker marca 'dormindo' e, sem contato depois do prazo + tolerancia, 'offline'.\n", d)
	m.printf("  A proxima leitura (ou 'conectar') acorda o sensor e o broker publica 'online'.\n")
}

func (m *Manual) burst(ctx context.Context, args []string) {
	if len(args) < 1 || len(args) > 2 {
		m.printf("  uso: rajada <n> [temperatura]\n")
		return
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n <= 0 {
		m.printf("  quantidade invalida %q\n", args[0])
		return
	}
	temperature := float32(25)
	if len(args) == 2 {
		v, err := strconv.ParseFloat(strings.ReplaceAll(args[1], ",", "."), 32)
		if err != nil {
			m.printf("  temperatura invalida %q\n", args[1])
			return
		}
		temperature = float32(v)
	}
	now := time.Now()
	for i := 0; i < n; i++ {
		if _, err := m.buffer.Append(now, temperature); err != nil {
			m.printf("  erro ao gravar no buffer: %v\n", err)
			return
		}
	}
	st := m.buffer.Stats()
	m.printf("  %d leituras gravadas no buffer (pendentes: %d, descartes por buffer cheio: %d)\n", n, st.Pending, st.Dropped)
	if m.client == nil {
		m.printf("  desconectado: serao enviadas ao conectar\n")
		return
	}
	start := time.Now()
	flushCtx, cancel := context.WithTimeout(ctx, burstFlushTimeout)
	defer cancel()
	if err := m.client.FlushPending(flushCtx); err != nil {
		m.printf("  envio parcial (%v); pendentes: %d\n", shortErr(err), m.buffer.Stats().Pending)
		return
	}
	m.printf("  rajada enviada em %s; pendentes: %d\n", time.Since(start).Round(time.Millisecond), m.buffer.Stats().Pending)
}

func (m *Manual) status() {
	st := m.buffer.Stats()
	conn := "desconectado"
	if m.client != nil {
		conn = "conectado"
		if !m.client.Connected() {
			conn = "reconectando"
		}
	}
	m.printf("  %s a %s | ultimo seq: %d | pendentes no buffer: %d | descartes por buffer cheio: %d\n",
		conn, m.cfg.Addr, st.LastSeq, st.Pending, st.Dropped)
}

func (m *Manual) pending() {
	readings := m.buffer.Pending()
	if len(readings) == 0 {
		m.printf("  buffer vazio\n")
		return
	}
	const maxShown = 20
	for i, r := range readings {
		if i == maxShown {
			m.printf("  ... e mais %d\n", len(readings)-maxShown)
			break
		}
		m.printf("  seq=%d  %.2f  medida ha %s\n", r.Seq, r.Temperature, r.Age(time.Now()).Round(time.Second))
	}
}
