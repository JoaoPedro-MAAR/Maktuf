// Package sensor implementa o simulador de sensores de temperatura do Maktuf
// (RF-59). Cada sensor simulado mede em intervalo fixo, grava a leitura no
// buffer durável e a publica pela biblioteca cliente. Desconexões e ciclos
// de sono são sorteados a partir de uma semente fixa, para que o cenário seja
// reproduzível por configuração (RNF-22).
package sensor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

// Valores padrão do perfil de referência (RNF-03). DefaultBufferSize é
// provisório: o limite do buffer do sensor ainda é ponto em aberto
// (requisitos.md, 11.1 item 5).
const (
	DefaultIDPrefix           = "sensor"
	DefaultBufferSize         = 10000
	DefaultContinuousRatio    = 0.1
	DefaultLowInterval        = 5 * time.Minute
	DefaultContinuousInterval = time.Second
	DefaultFailureDowntime    = 10 * time.Second
	DefaultSleepDuration      = 30 * time.Second
	DefaultLateWakeExtra      = 30 * time.Second
	DefaultPublishTimeout     = 5 * time.Second
	DefaultDrainTimeout       = 2 * time.Second

	// burstFlushTimeout limita o envio imediato da rajada; o resto segue no
	// buffer.
	burstFlushTimeout = 30 * time.Second
)

// ErrInvalidConfig indica uma configuração do simulador fora dos limites.
var ErrInvalidConfig = errors.New("sensor: configuracao invalida")

// Profile é o perfil de taxa de um sensor simulado.
type Profile int

const (
	// ProfileLow mede a cada LowInterval (1 leitura a cada 5 min no RNF-03).
	ProfileLow Profile = iota
	// ProfileContinuous mede a cada ContinuousInterval (1 leitura/s no RNF-03).
	ProfileContinuous
)

func (p Profile) String() string {
	if p == ProfileContinuous {
		return "continuo"
	}
	return "baixo"
}

// Config descreve o cenário simulado. Os campos de duração e de tamanho com
// valor zero recebem os defaults acima; as probabilidades zero desligam o
// comportamento associado.
type Config struct {
	// Addr é o endereço TCP do broker.
	Addr string
	// Count é a quantidade de sensores simulados.
	Count int
	// IDPrefix forma os identificadores: <prefixo>-001, <prefixo>-002, ...
	IDPrefix string
	// DataDir guarda um arquivo de buffer por sensor (<id>.buf). Reiniciar o
	// simulador com o mesmo diretório e os mesmos ids retoma as pendências.
	DataDir string
	// BufferSize é o máximo de leituras pendentes por sensor.
	BufferSize int

	// ContinuousRatio é a fração de sensores com perfil contínuo.
	ContinuousRatio    float64
	LowInterval        time.Duration
	ContinuousInterval time.Duration
	// Stagger espalha a primeira medição de cada sensor dentro do seu
	// intervalo, evitando que todos publiquem no mesmo instante.
	Stagger bool

	// Seed alimenta os sorteios. Cada sensor usa Seed + índice.
	Seed int64

	// FailureProb é a chance, por medição, de o sensor perder a conexão. Ele
	// fica FailureDowntime sem conectar, acumulando leituras no buffer.
	FailureProb     float64
	FailureDowntime time.Duration

	// SleepProb é a chance, por medição, de o sensor anunciar SLEEP e dormir
	// por SleepDuration. Com chance LateWakeProb ele acorda LateWakeExtra
	// depois do prazo, o que leva o broker a publicar offline.
	SleepProb     float64
	SleepDuration time.Duration
	LateWakeProb  float64
	LateWakeExtra time.Duration

	// PublishTimeout limita a espera pelo PUBACK de cada medição. Ao estourar,
	// a leitura continua no buffer e é reenviada depois.
	PublishTimeout time.Duration

	// BurstAt e BurstSize geram uma rajada (RF-35, RNF-04): BurstAt depois
	// do início, BurstSize leituras no total, divididas entre os sensores,
	// são gravadas de uma vez no buffer e enviadas em seguida. BurstSize zero
	// desliga.
	BurstAt   time.Duration
	BurstSize int

	// DrainTimeout é quanto cada sensor conectado espera, ao encerrar, para
	// enviar as leituras ainda pendentes. Zero encerra sem drenar; o que
	// ficar no buffer é enviado na próxima execução.
	DrainTimeout time.Duration

	Reconnect client.ReconnectOptions
	// Metrics recebe as métricas do simulador. Nil cria um registro
	// próprio, acessível por Simulator.Metrics.
	Metrics *metrics.Registry
	Logger  *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.IDPrefix == "" {
		c.IDPrefix = DefaultIDPrefix
	}
	if c.BufferSize == 0 {
		c.BufferSize = DefaultBufferSize
	}
	if c.LowInterval == 0 {
		c.LowInterval = DefaultLowInterval
	}
	if c.ContinuousInterval == 0 {
		c.ContinuousInterval = DefaultContinuousInterval
	}
	if c.FailureDowntime == 0 {
		c.FailureDowntime = DefaultFailureDowntime
	}
	if c.SleepDuration == 0 {
		c.SleepDuration = DefaultSleepDuration
	}
	if c.LateWakeExtra == 0 {
		c.LateWakeExtra = DefaultLateWakeExtra
	}
	if c.PublishTimeout == 0 {
		c.PublishTimeout = DefaultPublishTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if c.Metrics == nil {
		c.Metrics = metrics.NewRegistry()
	}
	return c
}

func (c Config) validate() error {
	switch {
	case c.Addr == "":
		return fmt.Errorf("%w: endereco do broker vazio", ErrInvalidConfig)
	case c.Count <= 0:
		return fmt.Errorf("%w: quantidade de sensores deve ser positiva", ErrInvalidConfig)
	case c.DataDir == "":
		return fmt.Errorf("%w: diretorio de dados vazio", ErrInvalidConfig)
	case c.BufferSize < 0:
		return fmt.Errorf("%w: tamanho do buffer negativo", ErrInvalidConfig)
	case c.LowInterval < 0 || c.ContinuousInterval < 0:
		return fmt.Errorf("%w: intervalo negativo", ErrInvalidConfig)
	case c.FailureDowntime < 0 || c.SleepDuration < 0 || c.LateWakeExtra < 0 || c.PublishTimeout < 0 || c.DrainTimeout < 0 || c.BurstAt < 0:
		return fmt.Errorf("%w: duracao negativa", ErrInvalidConfig)
	case c.BurstSize < 0:
		return fmt.Errorf("%w: tamanho da rajada negativo", ErrInvalidConfig)
	}
	probs := map[string]float64{
		"continuous-ratio": c.ContinuousRatio,
		"failure-prob":     c.FailureProb,
		"sleep-prob":       c.SleepProb,
		"late-wake-prob":   c.LateWakeProb,
	}
	for name, p := range probs {
		if p < 0 || p > 1 || math.IsNaN(p) {
			return fmt.Errorf("%w: %s = %v fora de [0, 1]", ErrInvalidConfig, name, p)
		}
	}
	return nil
}

// Spec identifica um sensor simulado.
type Spec struct {
	ID       string
	Topic    string
	Profile  Profile
	Interval time.Duration
}

// Specs devolve os sensores do cenário. Os primeiros round(Count *
// ContinuousRatio) sensores têm perfil contínuo; os demais, perfil baixo.
func Specs(cfg Config) []Spec {
	cfg = cfg.withDefaults()
	continuous := int(math.Round(float64(cfg.Count) * cfg.ContinuousRatio))
	specs := make([]Spec, cfg.Count)
	for i := range specs {
		id := fmt.Sprintf("%s-%03d", cfg.IDPrefix, i+1)
		spec := Spec{
			ID:       id,
			Topic:    "sensores/" + id + "/temperatura",
			Profile:  ProfileLow,
			Interval: cfg.LowInterval,
		}
		if i < continuous {
			spec.Profile = ProfileContinuous
			spec.Interval = cfg.ContinuousInterval
		}
		specs[i] = spec
	}
	return specs
}

// Stats resume o simulador. Persisted e NoSubscribers contam o status do
// PUBACK de cada medição na hora em que ela foi feita; leituras reenviadas
// depois pela biblioteca entram em Confirmed, calculado pelo buffer.
type Stats struct {
	Sensors int
	// Measured é o total de leituras geradas.
	Measured      uint64
	Persisted     uint64
	NoSubscribers uint64
	// PublishTimeouts conta medições que ficaram no buffer porque o PUBACK
	// não chegou a tempo.
	PublishTimeouts uint64
	// Confirmed é o total de leituras removidas do buffer por resposta
	// definitiva do broker (PERSISTED ou NO_SUBSCRIBERS).
	Confirmed uint64
	// Pending é a soma das leituras ainda no buffer de cada sensor.
	Pending int
	// Dropped é a soma dos descartes por overflow do buffer (RF-26, RF-31).
	Dropped uint64

	Connects        uint64
	ConnectFailures uint64
	Failures        uint64
	Sleeps          uint64
	LateWakes       uint64
}

type counters struct {
	measured        atomic.Uint64
	persisted       atomic.Uint64
	noSubscribers   atomic.Uint64
	publishTimeouts atomic.Uint64
	connects        atomic.Uint64
	connectFailures atomic.Uint64
	failures        atomic.Uint64
	sleeps          atomic.Uint64
	lateWakes       atomic.Uint64
	burstReadings   atomic.Uint64
}

// Simulator executa vários sensores simulados, um por goroutine.
type Simulator struct {
	cfg   Config
	specs []Spec
	stats counters

	mu      sync.Mutex
	buffers []*storage.FileBuffer
	running bool
}

// New valida a configuração e prepara o simulador.
func New(cfg Config) (*Simulator, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()
	s := &Simulator{cfg: cfg, specs: Specs(cfg)}
	registerMetrics(cfg.Metrics, s)
	return s, nil
}

// Metrics devolve o registro com as métricas do simulador.
func (s *Simulator) Metrics() *metrics.Registry {
	return s.cfg.Metrics
}

// Specs devolve os sensores simulados.
func (s *Simulator) Specs() []Spec {
	out := make([]Spec, len(s.specs))
	copy(out, s.specs)
	return out
}

// Run abre os buffers, executa os sensores até o contexto terminar e então
// fecha conexões e buffers. O cancelamento do contexto é o encerramento
// normal e devolve nil.
func (s *Simulator) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("sensor: simulador ja esta em execucao")
	}
	s.running = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	if err := os.MkdirAll(s.cfg.DataDir, 0o750); err != nil {
		return fmt.Errorf("sensor: criar diretorio de dados: %w", err)
	}

	buffers := make([]*storage.FileBuffer, 0, len(s.specs))
	closeBuffers := func() {
		for _, b := range buffers {
			_ = b.Close()
		}
	}
	for _, spec := range s.specs {
		b, err := storage.OpenSensorBuffer(filepath.Join(s.cfg.DataDir, spec.ID+".buf"), storage.BufferOptions{
			MaxReadings: s.cfg.BufferSize,
			SensorID:    spec.ID,
			Logger:      s.cfg.Logger,
		})
		if err != nil {
			closeBuffers()
			return fmt.Errorf("sensor: abrir buffer de %s: %w", spec.ID, err)
		}
		buffers = append(buffers, b)
	}
	s.mu.Lock()
	s.buffers = buffers
	s.mu.Unlock()
	defer closeBuffers()

	var wg sync.WaitGroup
	sims := make([]*simSensor, len(s.specs))
	for i, spec := range s.specs {
		sim := &simSensor{
			spec:   spec,
			cfg:    &s.cfg,
			stats:  &s.stats,
			buffer: buffers[i],
			rng:    rand.New(rand.NewSource(s.cfg.Seed + int64(i))),
			log:    s.cfg.Logger.With("sensor_id", spec.ID, "topic", spec.Topic, "perfil", spec.Profile.String()),
			burstN: burstShare(s.cfg.BurstSize, len(s.specs), i),
		}
		if sim.burstN > 0 {
			sim.burstCh = make(chan struct{}, 1)
		}
		sims[i] = sim
		wg.Add(1)
		go func() {
			defer wg.Done()
			sim.run(ctx)
		}()
	}
	if s.cfg.BurstSize > 0 {
		go triggerBurst(ctx, s.cfg.BurstAt, sims)
	}
	wg.Wait()
	return nil
}

// burstShare divide total entre n sensores: os primeiros recebem um a mais.
func burstShare(total, n, i int) int {
	share := total / n
	if i < total%n {
		share++
	}
	return share
}

// triggerBurst avisa os sensores no instante da rajada.
func triggerBurst(ctx context.Context, at time.Duration, sims []*simSensor) {
	t := time.NewTimer(at)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
		return
	}
	for _, sim := range sims {
		if sim.burstCh != nil {
			sim.burstCh <- struct{}{}
		}
	}
}

// Stats devolve um retrato dos contadores e dos buffers. Pode ser chamado
// durante ou depois de Run.
func (s *Simulator) Stats() Stats {
	st := Stats{
		Sensors:         len(s.specs),
		Measured:        s.stats.measured.Load(),
		Persisted:       s.stats.persisted.Load(),
		NoSubscribers:   s.stats.noSubscribers.Load(),
		PublishTimeouts: s.stats.publishTimeouts.Load(),
		Connects:        s.stats.connects.Load(),
		ConnectFailures: s.stats.connectFailures.Load(),
		Failures:        s.stats.failures.Load(),
		Sleeps:          s.stats.sleeps.Load(),
		LateWakes:       s.stats.lateWakes.Load(),
	}
	for _, b := range s.bufferStats() {
		st.Pending += b.pending
		st.Dropped += b.dropped
		st.Confirmed += b.confirmed
	}
	return st
}

type sensorBufferStats struct {
	id        string
	pending   int
	dropped   uint64
	confirmed uint64
}

// bufferStats lê o buffer de cada sensor. Vazio antes de Run abrir os
// buffers.
func (s *Simulator) bufferStats() []sensorBufferStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sensorBufferStats, len(s.buffers))
	for i, b := range s.buffers {
		bs := b.Stats()
		out[i] = sensorBufferStats{
			id:      s.specs[i].ID,
			pending: bs.Pending,
			dropped: bs.Dropped,
			// LastSeq é o total de leituras já geradas pelo sensor, inclusive
			// em execuções anteriores do simulador.
			confirmed: bs.LastSeq - uint64(bs.Pending) - bs.Dropped,
		}
	}
	return out
}

// simSensor é o laço de um sensor. Só a própria goroutine acessa seus campos.
type simSensor struct {
	spec   Spec
	cfg    *Config
	stats  *counters
	buffer *storage.FileBuffer
	rng    *rand.Rand
	log    *slog.Logger

	client       *client.Client
	offlineUntil time.Time
	base         float64
	temperature  float64

	// burstN é a parte deste sensor na rajada; burstCh recebe o sinal.
	burstN  int
	burstCh chan struct{}
}

func (s *simSensor) run(ctx context.Context) {
	defer s.shutdown()

	s.base = 20 + s.rng.Float64()*10
	s.temperature = s.base

	var start time.Duration
	if s.cfg.Stagger {
		start = time.Duration(s.rng.Int63n(int64(s.spec.Interval) + 1))
	}
	if !s.pause(ctx, start) {
		return
	}

	next := time.Now()
	for {
		if !s.measure(ctx) {
			return
		}

		// Os sorteios acontecem sempre, na mesma ordem, para que a sequência
		// de eventos dependa só da semente e não do estado da conexão.
		failRoll, sleepRoll, lateRoll := s.rng.Float64(), s.rng.Float64(), s.rng.Float64()

		if failRoll < s.cfg.FailureProb {
			s.simulateFailure()
		}
		if sleepRoll < s.cfg.SleepProb {
			if !s.sleep(ctx, lateRoll < s.cfg.LateWakeProb) {
				return
			}
			next = time.Now()
			continue
		}

		next = next.Add(s.spec.Interval)
		if now := time.Now(); next.Before(now) {
			// Sem rajada para recuperar medições atrasadas.
			next = now
		}
		if !s.pause(ctx, time.Until(next)) {
			return
		}
	}
}

// pause espera d como wait, mas atende a rajada se ela chegar no meio.
func (s *simSensor) pause(ctx context.Context, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return ctx.Err() == nil
		}
		t := time.NewTimer(remaining)
		select {
		case <-t.C:
			return true
		case <-ctx.Done():
			t.Stop()
			return false
		case <-s.burstCh:
			t.Stop()
			if !s.burst(ctx) {
				return false
			}
		}
	}
}

// burst grava as leituras da rajada no buffer de uma vez e tenta enviá-las.
// O que não sair agora continua no buffer e vai antes da próxima medição.
func (s *simSensor) burst(ctx context.Context) bool {
	now := time.Now()
	for i := 0; i < s.burstN; i++ {
		temperature := s.nextTemperature()
		if _, err := s.buffer.Append(now, temperature); err != nil {
			s.log.Error("falha ao gravar leitura da rajada no buffer", "error", err)
			return false
		}
		s.stats.measured.Add(1)
		s.stats.burstReadings.Add(1)
	}
	s.log.Info("rajada gerada", "leituras", s.burstN, "pendentes", s.buffer.Stats().Pending)
	if s.client == nil {
		return true
	}
	flushCtx, cancel := context.WithTimeout(ctx, burstFlushTimeout)
	defer cancel()
	if err := s.client.FlushPending(flushCtx); err != nil && ctx.Err() == nil {
		s.log.Warn("rajada ainda no buffer", "pendentes", s.buffer.Stats().Pending, "error", err)
	}
	return ctx.Err() == nil
}

// measure gera uma leitura e a entrega à biblioteca. Desconectado, o sensor
// apenas grava no buffer; a biblioteca reenvia as pendências em ordem de seq
// quando a conexão voltar (RF-23, RF-25, CE-03). Devolve false se o sensor
// deve parar.
func (s *simSensor) measure(ctx context.Context) bool {
	now := time.Now()
	temperature := s.nextTemperature()
	s.stats.measured.Add(1)

	if s.client == nil && !now.Before(s.offlineUntil) {
		s.connect(ctx)
	}
	if s.client == nil {
		r, err := s.buffer.Append(now, temperature)
		if err != nil {
			s.log.Error("falha ao gravar leitura no buffer", "error", err)
			return false
		}
		s.log.Debug("leitura guardada no buffer (desconectado)", "seq", r.Seq, "temperatura", temperature)
		return true
	}

	pubCtx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
	r, status, err := s.client.PublishBuffered(pubCtx, now, temperature)
	cancel()

	if err != nil {
		if r.Seq == 0 {
			// A leitura nem entrou no buffer: falha de disco.
			s.log.Error("falha ao gravar leitura no buffer", "error", err)
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		if s.client.Err() != nil {
			// Reconexão esgotada (MaxAttempts): a próxima medição conecta de novo.
			s.disconnect()
		}
		s.stats.publishTimeouts.Add(1)
		s.log.Warn("leitura mantida no buffer sem PUBACK", "seq", r.Seq, "error", err)
		return true
	}

	switch status {
	case client.StatusPersisted:
		s.stats.persisted.Add(1)
		s.log.Debug("leitura confirmada", "seq", r.Seq, "temperatura", temperature)
	case client.StatusNoSubscribers:
		s.stats.noSubscribers.Add(1)
		s.log.Info("leitura descartada pelo broker sem inscritos", "seq", r.Seq)
	}
	return true
}

func (s *simSensor) connect(ctx context.Context) {
	c, err := client.ConnectWithOptions(ctx, s.cfg.Addr, client.RoleSensor, s.spec.ID, client.Options{
		Reconnect: s.cfg.Reconnect,
		Sensor:    &client.SensorOptions{Buffer: s.buffer, Topic: s.spec.Topic},
	})
	if err != nil {
		s.stats.connectFailures.Add(1)
		s.log.Debug("conexao com o broker falhou", "error", err)
		return
	}
	s.client = c
	s.stats.connects.Add(1)
	s.log.Info("conectado ao broker", "pendentes", s.buffer.Stats().Pending)
}

// shutdown tenta drenar o buffer antes de fechar a conexão. Usa um contexto
// novo porque o da execução já foi cancelado.
func (s *simSensor) shutdown() {
	if s.cfg.DrainTimeout > 0 && s.buffer.Stats().Pending > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.DrainTimeout)
		// Desconectado (downtime simulado ou sono), conecta só para drenar:
		// o encerramento não é uma falha, então não deve deixar backlog.
		if s.client == nil {
			s.connect(ctx)
		}
		if s.client != nil {
			if err := s.client.FlushPending(ctx); err != nil {
				s.log.Warn("encerrando com leituras pendentes no buffer",
					"pendentes", s.buffer.Stats().Pending, "error", err)
			}
		}
		cancel()
	}
	s.disconnect()
}

func (s *simSensor) disconnect() {
	if s.client != nil {
		_ = s.client.Close()
		s.client = nil
	}
}

// simulateFailure derruba a conexão sem SLEEP. Para o broker é uma queda
// comum; o sensor continua medindo e só tenta reconectar depois do downtime.
func (s *simSensor) simulateFailure() {
	if s.client == nil {
		return
	}
	s.disconnect()
	s.offlineUntil = time.Now().Add(s.cfg.FailureDowntime)
	s.stats.failures.Add(1)
	s.log.Info("desconexao simulada", "downtime", s.cfg.FailureDowntime)
}

// sleep anuncia SLEEP, desconecta e dorme sem medir (RF-27). Acordar tarde
// faz o broker publicar offline (RF-28); a reconexão seguinte publica online
// (RF-29).
func (s *simSensor) sleep(ctx context.Context, late bool) bool {
	duration := s.cfg.SleepDuration
	if s.client != nil {
		sleepCtx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
		err := s.client.Sleep(sleepCtx, duration)
		cancel()
		if err != nil {
			s.log.Warn("SLEEP nao enviado", "error", err)
		}
		s.disconnect()
	}

	s.stats.sleeps.Add(1)
	if late {
		s.stats.lateWakes.Add(1)
		duration += s.cfg.LateWakeExtra
	}
	s.log.Info("sensor dormindo", "duracao", s.cfg.SleepDuration, "acorda_atrasado", late)
	if !wait(ctx, duration) {
		return false
	}
	s.offlineUntil = time.Time{}
	return true
}

// nextTemperature faz um passeio aleatório que volta devagar para a base do
// sensor, para os valores parecerem uma medição real.
func (s *simSensor) nextTemperature() float32 {
	s.temperature += (s.base-s.temperature)*0.05 + s.rng.NormFloat64()*0.3
	return float32(math.Round(s.temperature*100) / 100)
}

func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
