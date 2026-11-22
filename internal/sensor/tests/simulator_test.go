package sensor_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
)

const timeout = 10 * time.Second

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

var fastReconnect = client.ReconnectOptions{
	BaseDelay:   10 * time.Millisecond,
	MaxDelay:    50 * time.Millisecond,
	DialTimeout: time.Second,
}

type testBroker struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func startBroker(t *testing.T, addr string, cfg broker.Config) *testBroker {
	t.Helper()
	cfg.Addr = addr
	cfg.Logger = discard
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	srv := broker.NewServer(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &testBroker{cancel: cancel, done: make(chan error, 1)}
	go func() { b.done <- srv.Serve(ctx) }()
	t.Cleanup(func() { b.stop(t) })
	return b
}

func (b *testBroker) stop(t *testing.T) {
	t.Helper()
	b.once.Do(func() {
		b.cancel()
		select {
		case err := <-b.done:
			if err != nil {
				t.Errorf("shutdown do broker: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout aguardando shutdown do broker")
		}
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// startConsumer roda o consumidor de exemplo e espera as inscrições.
func startConsumer(t *testing.T, addr string) *consumer.Consumer {
	t.Helper()
	return startConsumerWith(t, consumer.Config{Addr: addr})
}

func startConsumerWith(t *testing.T, cfg consumer.Config) *consumer.Consumer {
	t.Helper()
	cfg.RetryInterval = 10 * time.Millisecond
	cfg.Reconnect = fastReconnect
	cfg.Logger = discard
	c, err := consumer.New(cfg)
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("consumidor: %v", err)
		}
	})
	select {
	case <-c.Ready():
	case <-time.After(timeout):
		t.Fatalf("timeout aguardando inscricoes do consumidor")
	}
	return c
}

// runSimulator executa o simulador até until devolver true e o encerra.
func runSimulator(t *testing.T, cfg sensor.Config, until func(sensor.Stats) bool) *sensor.Simulator {
	t.Helper()
	cfg.Logger = discard
	cfg.Reconnect = fastReconnect
	sim, err := sensor.New(cfg)
	if err != nil {
		t.Fatalf("sensor.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sim.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for !until(sim.Stats()) {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("timeout; stats = %+v", sim.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	return sim
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// connectedAll diz que todos os sensores já conectaram. Measured sobe antes
// da conexão, então parar só por Measured cancelaria conexões em andamento e
// deixaria o backlog sem drenar.
func connectedAll(s sensor.Stats, cfg sensor.Config) bool {
	return s.Connects >= uint64(cfg.Count) && s.Measured >= uint64(cfg.Count)
}

func baseConfig(t *testing.T, addr string) sensor.Config {
	return sensor.Config{
		Addr:               addr,
		Count:              3,
		DataDir:            t.TempDir(),
		BufferSize:         1000,
		ContinuousRatio:    1,
		ContinuousInterval: 10 * time.Millisecond,
		PublishTimeout:     time.Second,
		DrainTimeout:       2 * time.Second,
		Seed:               7,
	}
}

func TestSpecsSplitProfiles(t *testing.T) {
	specs := sensor.Specs(sensor.Config{Count: 10, ContinuousRatio: 0.1})
	if len(specs) != 10 {
		t.Fatalf("len = %d", len(specs))
	}
	if specs[0].ID != "sensor-001" || specs[0].Topic != "sensores/sensor-001/temperatura" {
		t.Fatalf("spec[0] = %+v", specs[0])
	}
	if specs[0].Profile != sensor.ProfileContinuous || specs[0].Interval != sensor.DefaultContinuousInterval {
		t.Fatalf("spec[0] deveria ser continuo: %+v", specs[0])
	}
	for _, s := range specs[1:] {
		if s.Profile != sensor.ProfileLow || s.Interval != sensor.DefaultLowInterval {
			t.Fatalf("%s deveria ser de baixa taxa: %+v", s.ID, s)
		}
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	valid := sensor.Config{Addr: "127.0.0.1:1", Count: 1, DataDir: t.TempDir()}
	cases := map[string]func(*sensor.Config){
		"sem endereco":       func(c *sensor.Config) { c.Addr = "" },
		"sem sensores":       func(c *sensor.Config) { c.Count = 0 },
		"sem diretorio":      func(c *sensor.Config) { c.DataDir = "" },
		"probabilidade > 1":  func(c *sensor.Config) { c.FailureProb = 1.5 },
		"fracao negativa":    func(c *sensor.Config) { c.ContinuousRatio = -0.1 },
		"buffer negativo":    func(c *sensor.Config) { c.BufferSize = -1 },
		"intervalo negativo": func(c *sensor.Config) { c.ContinuousInterval = -time.Second },
		"duracao sono negativa": func(c *sensor.Config) {
			c.SleepDuration = -time.Second
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			if _, err := sensor.New(cfg); !errors.Is(err, sensor.ErrInvalidConfig) {
				t.Fatalf("err = %v, esperado ErrInvalidConfig", err)
			}
		})
	}
	if _, err := sensor.New(valid); err != nil {
		t.Fatalf("config valida recusada: %v", err)
	}
}

// Fluxo normal: tudo que o simulador mede chega ao consumidor, em ordem e
// sem lacunas.
func TestSimulatorDeliversEveryReading(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	sim := runSimulator(t, baseConfig(t, addr), func(s sensor.Stats) bool { return s.Measured >= 30 })

	st := sim.Stats()
	if st.Pending != 0 || st.Dropped != 0 || st.Confirmed != st.Measured {
		t.Fatalf("buffer nao drenou: %+v", st)
	}
	if st.NoSubscribers != 0 {
		t.Fatalf("leituras sem inscritos com consumidor ativo: %+v", st)
	}
	waitFor(t, "consumidor receber tudo", func() bool { return cons.Stats().Received == st.Measured })
	if cs := cons.Stats(); cs.GapsUnmarked != 0 || cs.GapsMarked != 0 || cs.Sensors != 3 {
		t.Fatalf("consumidor = %+v", cs)
	}
}

// CE-03 / CT-05 e critério da task 016: com o broker fora, as leituras ficam
// no buffer em disco; elas sobrevivem a reiniciar o simulador e são
// reenviadas em ordem quando o broker volta.
func TestReadingsSurviveSimulatorRestartWhileBrokerIsDown(t *testing.T) {
	addr := freeAddr(t)
	cfg := baseConfig(t, addr)

	offline := runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Measured >= 15 })
	st := offline.Stats()
	if st.Pending != int(st.Measured) || st.Connects != 0 {
		t.Fatalf("sem broker tudo deveria ficar pendente: %+v", st)
	}

	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	previous := st.Measured
	online := runSimulator(t, cfg, func(s sensor.Stats) bool { return connectedAll(s, cfg) })
	if st := online.Stats(); st.Pending != 0 {
		t.Fatalf("backlog nao drenou: %+v", st)
	}
	total := previous + online.Stats().Measured

	waitFor(t, "consumidor receber o backlog", func() bool { return cons.Stats().Received == total })
	if cs := cons.Stats(); cs.GapsUnmarked != 0 {
		t.Fatalf("consumidor = %+v", cs)
	}
}

// RF-59: desconexões sorteadas não perdem leituras; o reenvio pode gerar
// leituras repetidas, que o consumidor descarta.
func TestSimulatedFailuresLoseNothing(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	cfg := baseConfig(t, addr)
	cfg.FailureProb = 0.3
	cfg.FailureDowntime = 30 * time.Millisecond

	sim := runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Failures >= 5 && s.Measured >= 40 })

	// Leituras medidas durante o downtime ficam no buffer até a próxima
	// conexão; roda de novo para drenar.
	cfg.FailureProb = 0
	drain := runSimulator(t, cfg, func(s sensor.Stats) bool { return connectedAll(s, cfg) })
	if st := drain.Stats(); st.Pending != 0 {
		t.Fatalf("backlog nao drenou: %+v", st)
	}
	total := sim.Stats().Measured + drain.Stats().Measured

	waitFor(t, "consumidor receber tudo", func() bool { return cons.Stats().Received == total })
	if cs := cons.Stats(); cs.GapsUnmarked != 0 {
		t.Fatalf("lacunas com desconexoes simuladas: %+v", cs)
	}
}

// RF-27 a RF-29: o sono anunciado aparece como dormindo e, ao acordar depois
// do prazo, como offline e depois online.
func TestSleepCyclePublishesStatus(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{
		StatusTolerance:    20 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	cons := startConsumer(t, addr)

	cfg := baseConfig(t, addr)
	cfg.Count = 1
	cfg.SleepProb = 1
	cfg.SleepDuration = 20 * time.Millisecond
	cfg.LateWakeProb = 1
	cfg.LateWakeExtra = 100 * time.Millisecond

	runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Sleeps >= 2 })

	waitFor(t, "transicoes de status", func() bool {
		cs := cons.Stats()
		return cs.StatusOnline >= 1 && cs.StatusDormindo >= 1 && cs.StatusOffline >= 1
	})
}

func TestBufferOverflowIsReported(t *testing.T) {
	addr := freeAddr(t)
	cfg := baseConfig(t, addr)
	cfg.Count = 1
	cfg.BufferSize = 5
	cfg.DrainTimeout = 0

	sim := runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Measured >= 12 })
	st := sim.Stats()
	if st.Pending != 5 || st.Dropped != st.Measured-5 {
		t.Fatalf("overflow (RF-26) = %+v", st)
	}
}

func metricValue(t *testing.T, reg *metrics.Registry, series string) float64 {
	t.Helper()
	var buf bytes.Buffer
	if err := metrics.WriteText(&buf, reg.Gather()); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	for _, line := range strings.Split(buf.String(), "\n") {
		if rest, ok := strings.CutPrefix(line, series+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("serie %s ausente:\n%s", series, buf.String())
	return 0
}

// Task 015: simulador e consumidor expõem as métricas do lado do cliente
// (overflow do buffer do sensor e deduplicação no consumidor).
func TestSimulatorAndConsumerExposeMetrics(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	sim := runSimulator(t, baseConfig(t, addr), func(s sensor.Stats) bool { return s.Measured >= 15 })
	st := sim.Stats()
	reg := sim.Metrics()

	if got := metricValue(t, reg, "sensor_readings_total"); got != float64(st.Measured) {
		t.Fatalf("sensor_readings_total = %v, esperado %d", got, st.Measured)
	}
	if got := metricValue(t, reg, "sensor_readings_confirmed_total"); got != float64(st.Confirmed) {
		t.Fatalf("sensor_readings_confirmed_total = %v, esperado %d", got, st.Confirmed)
	}
	for _, spec := range sim.Specs() {
		if got := metricValue(t, reg, `sensor_buffer_overflow_total{sensor_id="`+spec.ID+`"}`); got != 0 {
			t.Fatalf("overflow de %s = %v", spec.ID, got)
		}
	}

	waitFor(t, "consumidor receber tudo", func() bool { return cons.Stats().Received == st.Measured })
	if got := metricValue(t, cons.Metrics(), `messages_received_total{group="sensor"}`); got != float64(st.Measured) {
		t.Fatalf("messages_received_total = %v, esperado %d", got, st.Measured)
	}
	if got := metricValue(t, cons.Metrics(), `end_to_end_latency_seconds_count{group="sensor"}`); got != float64(st.Measured) {
		t.Fatalf("end_to_end_latency_seconds_count = %v, esperado %d", got, st.Measured)
	}
}

// Task 017 / RF-35: a rajada vai inteira para o buffer e chega toda ao
// consumidor, em ordem e sem lacunas.
func TestBurstIsAbsorbedByTheBuffer(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	cfg := baseConfig(t, addr)
	cfg.ContinuousInterval = time.Second
	cfg.BurstAt = 50 * time.Millisecond
	cfg.BurstSize = 100

	sim := runSimulator(t, cfg, func(s sensor.Stats) bool {
		return s.Measured >= 103 && s.Pending == 0 && s.Confirmed >= s.Measured
	})
	if got := metricValue(t, sim.Metrics(), "sensor_burst_readings_total"); got != 100 {
		t.Fatalf("sensor_burst_readings_total = %v, esperado 100", got)
	}
	st := sim.Stats()
	waitFor(t, "consumidor receber a rajada", func() bool { return cons.Stats().Received == st.Measured })
	if cs := cons.Stats(); cs.GapsUnmarked != 0 || cs.GapsMarked != 0 {
		t.Fatalf("lacunas na rajada: %+v", cs)
	}
}

// Task 017 / CT-10: ProcessDelay deixa o consumidor lento de propósito.
func TestProcessDelaySlowsTheConsumer(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	const delay = 30 * time.Millisecond
	cons := startConsumerWith(t, consumer.Config{Addr: addr, ProcessDelay: delay})

	cfg := baseConfig(t, addr)
	cfg.Count = 1
	cfg.ContinuousInterval = time.Second
	cfg.BurstAt = 10 * time.Millisecond
	cfg.BurstSize = 10
	start := time.Now()
	runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Measured >= 11 && s.Pending == 0 })
	waitFor(t, "consumidor receber tudo", func() bool { return cons.Stats().Received >= 11 })
	if elapsed := time.Since(start); elapsed < 10*delay {
		t.Fatalf("11 mensagens em %s; com atraso de %s deveriam levar ao menos %s", elapsed, delay, 10*delay)
	}
}

// Dois ou mais sensores podem publicar no mesmo tópico: cada um com o
// próprio sensor_id e seq, e o consumidor recebe os fluxos de todos.
func TestSensorsCanShareATopic(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumerWith(t, consumer.Config{Addr: addr, Filters: []string{"estacao/comum"}})

	cfg := baseConfig(t, addr)
	cfg.Topic = "estacao/comum"
	sim := runSimulator(t, cfg, func(s sensor.Stats) bool { return s.Measured >= 15 })

	for _, spec := range sim.Specs() {
		if spec.Topic != "estacao/comum" {
			t.Fatalf("%s publica em %q", spec.ID, spec.Topic)
		}
	}
	st := sim.Stats()
	if st.Pending != 0 {
		t.Fatalf("backlog nao drenou: %+v", st)
	}
	waitFor(t, "consumidor receber tudo", func() bool { return cons.Stats().Received == st.Measured })
	if cs := cons.Stats(); cs.Sensors != 3 || cs.GapsUnmarked != 0 {
		t.Fatalf("consumidor = %+v, esperado 3 sensores sem lacunas", cs)
	}
}

func TestTopicTemplate(t *testing.T) {
	if got := sensor.TopicFor("", "a"); got != "sensores/a/temperatura" {
		t.Fatalf("padrao = %q", got)
	}
	if got := sensor.TopicFor("x/{id}/y", "a"); got != "x/a/y" {
		t.Fatalf("modelo = %q", got)
	}
	for topic, want := range map[string]bool{"a/b": true, "a//b": false, "a/+": false, "a/#": false, "": false} {
		if got := sensor.ValidPublishTopic(topic); got != want {
			t.Errorf("ValidPublishTopic(%q) = %v", topic, got)
		}
	}
	if _, err := sensor.New(sensor.Config{Addr: "x:1", Count: 1, DataDir: t.TempDir(), Topic: "a/+/b"}); !errors.Is(err, sensor.ErrInvalidConfig) {
		t.Fatalf("topico invalido: %v", err)
	}
}
