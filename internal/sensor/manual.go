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
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

// ManualConfig configura um sensor controlado por comandos, feito para
// demonstrar o sistema ao vivo (terminal com Run ou o painel web).
type ManualConfig struct {
	Addr       string
	SensorID   string
	BufferPath string
	BufferSize int
	// Topic é o tópico das leituras com buffer durável; "{id}" vira o id do
	// sensor. Vazio usa sensores/<id>/temperatura.
	Topic string
	// PublishTimeout limita a espera pelo PUBACK de cada leitura.
	PublishTimeout time.Duration
	Reconnect      client.ReconnectOptions
	Logger         *slog.Logger
}

// ErrBusy indica que outra operação do mesmo sensor (por exemplo, uma
// rajada) ainda está em andamento.
var ErrBusy = errors.New("sensor: operacao em andamento")

// Manual é um único sensor dirigido por comandos. Usa só a biblioteca
// cliente e o buffer durável, como um sensor real. As operações exportadas
// podem ser chamadas de várias goroutines; operações do mesmo sensor não
// rodam ao mesmo tempo (a segunda recebe ErrBusy).
type Manual struct {
	cfg    ManualConfig
	topic  string
	buffer *storage.FileBuffer

	opMu sync.Mutex
	// extraSeq numera as leituras de "publicar" em outros tópicos, por
	// sensor_id de fluxo. Só é acessado com opMu.
	extraSeq map[string]uint64

	stateMu sync.Mutex
	client  *client.Client
	// offline é a queda simulada por "desconectar": as leituras vão para o
	// buffer até um "conectar" explícito. Depois de "dormir", não: a próxima
	// leitura acorda o sensor.
	offline  bool
	sleeping bool
	busy     string

	out io.Writer
}

// ManualState é um retrato do sensor para exibição.
type ManualState struct {
	ID         string `json:"id"`
	Topic      string `json:"topico"`
	Connection string `json:"conexao"`
	LastSeq    uint64 `json:"ultimo_seq"`
	Pending    int    `json:"pendentes"`
	Dropped    uint64 `json:"descartes"`
	Busy       string `json:"ocupado,omitempty"`
}

// Valores de ManualState.Connection.
const (
	ConnConnected    = "conectado"
	ConnReconnecting = "reconectando"
	ConnOffline      = "desconectado"
	ConnSleeping     = "dormindo"
)

const manualHelp = `comandos:
  <temperatura>         publica uma leitura (ex.: 23.5) no topico do sensor
  publicar <topico> <temp>  publica em outro topico (ex.: publicar sensores/sala-01/umidade 55),
                        sem buffer durável; usa um fluxo proprio <id>.<ultimo nivel>
  rajada <n> [temp]     grava n leituras de uma vez e envia
  dormir <duracao>      envia SLEEP e desconecta (ex.: dormir 30s); a proxima leitura reconecta
  desconectar           derruba a conexao sem avisar (falha); as leituras ficam no buffer
  conectar              reconecta e reenvia o que estiver pendente
  status                mostra conexao, ultimo seq e buffer
  pendentes             lista as leituras ainda no buffer
  ajuda                 mostra esta ajuda
  sair                  encerra
`

// printer escreve as mensagens de uma operação: no terminal (Run) ou num
// texto devolvido ao painel.
type printer func(format string, args ...any)

// NewManual abre o buffer do sensor. A conexão é feita no primeiro uso.
func NewManual(cfg ManualConfig) (*Manual, error) {
	switch {
	case cfg.Addr == "":
		return nil, fmt.Errorf("%w: endereco do broker vazio", ErrInvalidConfig)
	case cfg.SensorID == "" || strings.ContainsAny(cfg.SensorID, "/+# "):
		return nil, fmt.Errorf("%w: id do sensor %q", ErrInvalidConfig, cfg.SensorID)
	case cfg.BufferPath == "":
		return nil, fmt.Errorf("%w: arquivo do buffer vazio", ErrInvalidConfig)
	case cfg.Topic != "" && !ValidPublishTopic(strings.ReplaceAll(cfg.Topic, TopicIDPlaceholder, "x")):
		return nil, fmt.Errorf("%w: topico %q (use niveis separados por /, sem + nem #)", ErrInvalidConfig, cfg.Topic)
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
	return &Manual{cfg: cfg, topic: TopicFor(cfg.Topic, cfg.SensorID), buffer: buffer, extraSeq: map[string]uint64{}}, nil
}

// Close desconecta e fecha o buffer.
func (m *Manual) Close() error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	m.disconnect()
	return m.buffer.Close()
}

// ID devolve o id do sensor.
func (m *Manual) ID() string { return m.cfg.SensorID }

// State devolve um retrato do sensor. Não espera operações em andamento.
func (m *Manual) State() ManualState {
	st := m.buffer.Stats()
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	conn := ConnOffline
	switch {
	case m.client != nil && m.client.Connected():
		conn = ConnConnected
	case m.client != nil:
		conn = ConnReconnecting
	case m.sleeping:
		conn = ConnSleeping
	}
	return ManualState{
		ID: m.cfg.SensorID, Topic: m.topic, Connection: conn,
		LastSeq: st.LastSeq, Pending: st.Pending, Dropped: st.Dropped, Busy: m.busy,
	}
}

// capture devolve um printer que acumula o texto e uma função que o lê.
func capture() (printer, func() string) {
	var b strings.Builder
	return func(format string, args ...any) { _, _ = fmt.Fprintf(&b, format, args...) }, b.String
}

// begin reserva o sensor para uma operação; false se outra está em andamento.
func (m *Manual) begin(what string) bool {
	if !m.opMu.TryLock() {
		return false
	}
	m.stateMu.Lock()
	m.busy = what
	m.stateMu.Unlock()
	return true
}

func (m *Manual) end() {
	m.stateMu.Lock()
	m.busy = ""
	m.stateMu.Unlock()
	m.opMu.Unlock()
}

// Connect conecta (ou explica por que a conexão atual está caída) e desfaz o
// "desconectar".
func (m *Manual) Connect(ctx context.Context) (string, error) {
	if !m.begin("conectando") {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.doConnect(ctx, p)
	return text(), nil
}

// Disconnect derruba a conexão sem SLEEP, como uma falha de rede.
func (m *Manual) Disconnect() (string, error) {
	if !m.begin("desconectando") {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.doDisconnect(p)
	return text(), nil
}

// Publish publica uma leitura (ou a guarda no buffer, se desconectado).
func (m *Manual) Publish(ctx context.Context, temperature float32) (string, error) {
	if !m.begin("publicando") {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.publish(ctx, p, temperature)
	return text(), nil
}

// PublishTo publica uma leitura em outro tópico pela mesma conexão. Sem buffer
// durável: a leitura só sai se houver conexão e PUBACK. O broker numera por
// sensor_id, então a leitura usa um fluxo próprio (<id>.<último nível do
// tópico>) com seq iniciado no relógio, para não colidir com o seq do buffer.
func (m *Manual) PublishTo(ctx context.Context, topic string, temperature float32) (string, error) {
	if !m.begin("publicando") {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.publishTo(ctx, p, topic, temperature)
	return text(), nil
}

// Sleep envia SLEEP(d) e desconecta.
func (m *Manual) Sleep(ctx context.Context, d time.Duration) (string, error) {
	if !m.begin("dormindo") {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.sleep(ctx, p, d)
	return text(), nil
}

// Burst grava n leituras de uma vez no buffer e tenta enviá-las.
func (m *Manual) Burst(ctx context.Context, n int, temperature float32) (string, error) {
	if !m.begin(fmt.Sprintf("rajada de %d", n)) {
		return "", ErrBusy
	}
	defer m.end()
	p, text := capture()
	m.burst(ctx, p, n, temperature)
	return text(), nil
}

// Run lê um comando por linha de in e escreve as respostas em out, até
// "sair", fim da entrada ou ctx terminar.
func (m *Manual) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	m.out = out
	m.printf("sensor %s -> %s (topico %s)\n", m.cfg.SensorID, m.cfg.Addr, m.topic)
	if st := m.buffer.Stats(); st.Pending > 0 || st.LastSeq > 0 {
		m.printf("buffer reaberto: ultimo seq %d, %d pendentes\n", st.LastSeq, st.Pending)
	}
	m.opMu.Lock()
	m.connect(ctx, m.printf)
	m.opMu.Unlock()
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
			m.opMu.Lock()
			keep := m.handle(ctx, strings.Fields(line))
			m.opMu.Unlock()
			if !keep {
				return nil
			}
		}
	}
}

func (m *Manual) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(m.out, format, args...)
}

// handle executa um comando do terminal; devolve false para encerrar. Roda
// com opMu.
func (m *Manual) handle(ctx context.Context, args []string) bool {
	if len(args) == 0 {
		return true
	}
	p := m.printf
	switch cmd := strings.ToLower(args[0]); cmd {
	case "sair", "exit", "quit":
		return false
	case "ajuda", "help", "?":
		p("%s", manualHelp)
	case "status":
		m.status(p)
	case "pendentes":
		m.pending(p)
	case "conectar":
		m.doConnect(ctx, p)
	case "desconectar":
		m.doDisconnect(p)
	case "publicar":
		if len(args) != 3 {
			p("  uso: publicar <topico> <temperatura>, por exemplo: publicar sensores/%s/umidade 55\n", m.cfg.SensorID)
			return true
		}
		temperature, err := ParseTemperature(args[2])
		if err != nil {
			p("  temperatura invalida %q\n", args[2])
			return true
		}
		m.publishTo(ctx, p, args[1], temperature)
	case "dormir":
		if len(args) != 2 {
			p("  uso: dormir <duracao>, por exemplo: dormir 30s\n")
			return true
		}
		d, err := time.ParseDuration(args[1])
		if err != nil || d <= 0 {
			p("  duracao invalida %q (use 30s, 2m...)\n", args[1])
			return true
		}
		m.sleep(ctx, p, d)
	case "rajada":
		if len(args) < 2 || len(args) > 3 {
			p("  uso: rajada <n> [temperatura]\n")
			return true
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n <= 0 {
			p("  quantidade invalida %q\n", args[1])
			return true
		}
		temperature := float32(25)
		if len(args) == 3 {
			v, err := ParseTemperature(args[2])
			if err != nil {
				p("  temperatura invalida %q\n", args[2])
				return true
			}
			temperature = v
		}
		m.burst(ctx, p, n, temperature)
	default:
		temperature, err := ParseTemperature(cmd)
		if err != nil {
			p("  comando desconhecido %q; digite 'ajuda'\n", args[0])
			return true
		}
		m.publish(ctx, p, temperature)
	}
	return true
}

// ParseTemperature aceita "23.5" ou "23,5".
func ParseTemperature(s string) (float32, error) {
	v, err := strconv.ParseFloat(strings.ReplaceAll(strings.TrimSpace(s), ",", "."), 32)
	return float32(v), err
}

func (m *Manual) setClient(c *client.Client) {
	m.stateMu.Lock()
	m.client = c
	if c != nil {
		m.sleeping = false
	}
	m.stateMu.Unlock()
}

func (m *Manual) setOffline(offline bool) {
	m.stateMu.Lock()
	m.offline = offline
	m.stateMu.Unlock()
}

func (m *Manual) doConnect(ctx context.Context, p printer) {
	m.setOffline(false)
	if m.client != nil {
		if m.client.Connected() {
			p("  ja conectado\n")
		} else {
			p("  a conexao caiu e a biblioteca esta tentando reconectar a %s sozinha, ainda sem sucesso.\n", m.cfg.Addr)
			p("  Se esse endereco for um broker que virou reserva, saia e use o vip (-addr 127.0.0.1:7070)\n")
		}
		return
	}
	m.connect(ctx, p)
}

func (m *Manual) doDisconnect(p printer) {
	if m.client == nil {
		p("  ja desconectado\n")
		return
	}
	m.disconnect()
	m.setOffline(true)
	p("  conexao derrubada sem SLEEP; para o broker e uma falha. Leituras vao para o buffer ate 'conectar'\n")
}

func (m *Manual) connect(ctx context.Context, p printer) {
	dialCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	c, err := client.ConnectWithOptions(dialCtx, m.cfg.Addr, client.RoleSensor, m.cfg.SensorID, client.Options{
		Reconnect: m.cfg.Reconnect,
		Sensor:    &client.SensorOptions{Buffer: m.buffer, Topic: m.topic},
	})
	if err != nil {
		p("  sem conexao com %s (%v); as leituras ficam no buffer\n", m.cfg.Addr, err)
		return
	}
	m.setClient(c)
	pending := m.buffer.Stats().Pending
	p("  conectado (broker publica 'online' no topico de status)\n")
	if pending > 0 {
		p("  reenviando %d leituras pendentes, em ordem de seq\n", pending)
	}
}

func (m *Manual) disconnect() {
	if m.client != nil {
		_ = m.client.Close()
		m.setClient(nil)
	}
}

func (m *Manual) publish(ctx context.Context, p printer, temperature float32) {
	if m.client == nil && !m.offline {
		m.connect(ctx, p)
	}
	if m.client == nil {
		r, err := m.buffer.Append(time.Now(), temperature)
		if err != nil {
			p("  erro ao gravar no buffer: %v\n", err)
			return
		}
		p("  seq=%d guardada no buffer (desconectado); pendentes: %d\n", r.Seq, m.buffer.Stats().Pending)
		return
	}
	pubCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	r, status, err := m.client.PublishBuffered(pubCtx, time.Now(), temperature)
	switch {
	case err != nil && r.Seq == 0:
		p("  erro ao gravar no buffer: %v\n", err)
	case err != nil:
		p("  seq=%d sem PUBACK por enquanto (%v); fica no buffer e sera reenviada. Pendentes: %d\n",
			r.Seq, shortErr(err), m.buffer.Stats().Pending)
		if !m.client.Connected() {
			p("  (sem conexao: reconectando a %s)\n", m.cfg.Addr)
		}
	default:
		p("  seq=%d %s\n", r.Seq, describeStatus(status))
	}
}

func (m *Manual) publishTo(ctx context.Context, p printer, topic string, temperature float32) {
	if !ValidPublishTopic(topic) {
		p("  topico invalido %q: niveis separados por /, sem nivel vazio, sem + nem #\n", topic)
		return
	}
	if topic == m.topic {
		m.publish(ctx, p, temperature)
		return
	}
	if m.client == nil && !m.offline {
		m.connect(ctx, p)
	}
	if m.client == nil {
		p("  sem conexao: leituras em outro topico nao usam o buffer; nada foi enviado\n")
		return
	}
	levels := strings.Split(topic, "/")
	stream := m.cfg.SensorID + "." + levels[len(levels)-1]
	seq, ok := m.extraSeq[stream]
	if !ok {
		seq = uint64(time.Now().UnixMilli())
	}
	seq++
	m.extraSeq[stream] = seq
	pubCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	status, err := m.client.Publish(pubCtx, topic, client.Reading{SensorID: stream, Seq: seq, Temperature: temperature})
	if err != nil {
		p("  %s: sem PUBACK (%v); esta leitura nao tem buffer e foi perdida\n", topic, shortErr(err))
		return
	}
	p("  %s (fluxo %s, seq=%d) %s\n", topic, stream, seq, describeStatus(status))
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

func (m *Manual) sleep(ctx context.Context, p printer, d time.Duration) {
	if m.offline {
		p("  desconectado: use 'conectar' antes de dormir\n")
		return
	}
	if m.client == nil {
		m.connect(ctx, p)
	}
	if m.client == nil {
		p("  sem conexao: nao da para avisar o broker\n")
		return
	}
	sleepCtx, cancel := context.WithTimeout(ctx, m.cfg.PublishTimeout)
	defer cancel()
	if err := m.client.Sleep(sleepCtx, d); err != nil {
		p("  SLEEP nao enviado: %v\n", err)
		return
	}
	m.disconnect()
	m.stateMu.Lock()
	m.sleeping = true
	m.stateMu.Unlock()
	p("  SLEEP(%s) enviado e conexao fechada: o broker marca 'dormindo' e, sem contato depois do prazo + tolerancia, 'offline'.\n", d)
	p("  A proxima leitura (ou 'conectar') acorda o sensor e o broker publica 'online'.\n")
}

func (m *Manual) burst(ctx context.Context, p printer, n int, temperature float32) {
	if n <= 0 {
		p("  quantidade invalida %d\n", n)
		return
	}
	now := time.Now()
	for i := 0; i < n; i++ {
		if _, err := m.buffer.Append(now, temperature); err != nil {
			p("  erro ao gravar no buffer: %v\n", err)
			return
		}
	}
	st := m.buffer.Stats()
	p("  %d leituras gravadas no buffer (pendentes: %d, descartes por buffer cheio: %d)\n", n, st.Pending, st.Dropped)
	if m.client == nil {
		p("  desconectado: serao enviadas ao conectar\n")
		return
	}
	start := time.Now()
	flushCtx, cancel := context.WithTimeout(ctx, burstFlushTimeout)
	defer cancel()
	if err := m.client.FlushPending(flushCtx); err != nil {
		p("  envio parcial (%v); pendentes: %d\n", shortErr(err), m.buffer.Stats().Pending)
		return
	}
	p("  rajada enviada em %s; pendentes: %d\n", time.Since(start).Round(time.Millisecond), m.buffer.Stats().Pending)
}

func (m *Manual) status(p printer) {
	st := m.State()
	p("  %s a %s | ultimo seq: %d | pendentes no buffer: %d | descartes por buffer cheio: %d\n",
		st.Connection, m.cfg.Addr, st.LastSeq, st.Pending, st.Dropped)
}

func (m *Manual) pending(p printer) {
	readings := m.buffer.Pending()
	if len(readings) == 0 {
		p("  buffer vazio\n")
		return
	}
	const maxShown = 20
	for i, r := range readings {
		if i == maxShown {
			p("  ... e mais %d\n", len(readings)-maxShown)
			break
		}
		p("  seq=%d  %.2f  medida ha %s\n", r.Seq, r.Temperature, r.Age(time.Now()).Round(time.Second))
	}
}
