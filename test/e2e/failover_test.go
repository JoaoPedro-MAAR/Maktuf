package e2e_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/juiz"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

// Prazos curtos para o failover caber no CI. fence < failure, como o
// recomendado em produção.
const (
	judgeFailure  = 300 * time.Millisecond
	judgeCheck    = 20 * time.Millisecond
	heartbeatTick = 30 * time.Millisecond
	fenceTimeout  = 150 * time.Millisecond
	rtoLimit      = 30 * time.Second // RNF-06
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// startServer sobe um broker com a configuração dada em porta efêmera.
func startServer(t *testing.T, cfg broker.Config) (*broker.Server, *testBroker) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	cfg.Logger = quiet
	srv := broker.NewServer(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	b := &testBroker{addr: srv.Addr().String(), cancel: cancel, done: done}
	t.Cleanup(func() { b.stop(t) })
	return srv, b
}

// serve roda um componente com Serve(ctx) e o encerra no fim do teste.
func serve(t *testing.T, name string, run func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s nao encerrou", name)
		}
	})
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
	return 0
}

func waitFor(t *testing.T, what string, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type delivery struct {
	seq uint64
	at  time.Time
}

// CT-11, CT-12, RF-44, RF-45, RF-49, RNF-06, RNF-11: sensor e consumidor usam
// só o endereço do vip. O primário cai no meio do fluxo, o Juiz promove a
// reserva e os clientes seguem sozinhos: nenhuma leitura se perde, a ordem
// por sensor se mantém e não há novo SUBSCRIBE.
func TestFailoverIsTransparentToClients(t *testing.T) {
	judge, err := juiz.New(juiz.Config{
		Addr: "127.0.0.1:0", FailureTimeout: judgeFailure, CheckInterval: judgeCheck, Logger: quiet,
	})
	if err != nil {
		t.Fatalf("juiz.New: %v", err)
	}
	if err := judge.Listen(); err != nil {
		t.Fatalf("juiz Listen: %v", err)
	}
	serve(t, "juiz", judge.Serve)
	judgeStarted := time.Now()

	ha := func(cfg broker.Config) broker.Config {
		cfg.DataDir = t.TempDir()
		cfg.JudgeAddr = judge.Addr().String()
		cfg.HeartbeatInterval = heartbeatTick
		cfg.FenceTimeout = fenceTimeout
		return cfg
	}
	backupSrv, backup := startServer(t, ha(broker.Config{
		Role: broker.RoleBackup, ReplicationAddr: "127.0.0.1:0", BrokerID: "reserva",
	}))
	primarySrv, primary := startServer(t, ha(broker.Config{
		ReplicaAddr: backupSrv.ReplicationAddr().String(), ReplicaTimeout: 500 * time.Millisecond, BrokerID: "primario",
	}))
	waitFor(t, "reserva sincronizada", 5*time.Second, primarySrv.ReplicaSynced)

	proxy, err := vip.New(vip.Config{Addr: "127.0.0.1:0", Backends: []string{primary.addr, backup.addr}, Logger: quiet})
	if err != nil {
		t.Fatalf("vip.New: %v", err)
	}
	if err := proxy.Listen(); err != nil {
		t.Fatalf("vip Listen: %v", err)
	}
	serve(t, "vip", proxy.Serve)
	addr := proxy.Addr().String()

	// O Juiz só decide depois do período de aprendizado.
	time.Sleep(time.Until(judgeStarted.Add(judgeFailure + 3*judgeCheck)))

	reconnect := client.ReconnectOptions{BaseDelay: 10 * time.Millisecond, MaxDelay: 200 * time.Millisecond, DialTimeout: time.Second}
	setup, cancelSetup := context.WithTimeout(context.Background(), timeout)
	defer cancelSetup()

	consumer, err := client.ConnectWithOptions(setup, addr, client.RoleConsumer, "c1", client.Options{Reconnect: reconnect})
	if err != nil {
		t.Fatalf("consumidor via vip: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	if err := consumer.Subscribe(setup, topicA); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	buffer, err := storage.OpenSensorBuffer(filepath.Join(t.TempDir(), "a.buf"), storage.BufferOptions{
		MaxReadings: 1000, SensorID: "a", Logger: quiet,
	})
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	t.Cleanup(func() { _ = buffer.Close() })
	sensor, err := client.ConnectWithOptions(setup, addr, client.RoleSensor, "a", client.Options{
		Reconnect: reconnect,
		Sensor:    &client.SensorOptions{Buffer: buffer, Topic: topicA},
	})
	if err != nil {
		t.Fatalf("sensor via vip: %v", err)
	}
	t.Cleanup(func() { _ = sensor.Close() })

	// Coleta as entregas sem repetidas, na ordem de chegada.
	var (
		mu        sync.Mutex
		delivered []delivery
	)
	dedup := client.NewDedup(0)
	go func() {
		for m := range consumer.Messages() {
			if !dedup.AcceptReading(m) {
				continue
			}
			mu.Lock()
			delivered = append(delivered, delivery{seq: m.Seq, at: time.Now()})
			mu.Unlock()
		}
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(delivered)
	}

	const total, killAt = 60, 20
	var crashed time.Time
	for i := 1; i <= total; i++ {
		pubCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		// Erro aqui não é falha: a leitura fica no buffer e é reenviada.
		_, _, _ = sensor.PublishBuffered(pubCtx, time.Now(), 20+float32(i)/10)
		cancel()
		if i == killAt {
			waitFor(t, "entregas antes da queda", timeout, func() bool { return count() >= killAt/2 })
			crashed = time.Now()
			primary.stop(t)
		}
		time.Sleep(10 * time.Millisecond)
	}

	waitFor(t, "buffer do sensor vazio", 10*time.Second, func() bool { return buffer.Stats().Pending == 0 })
	waitFor(t, "todas as leituras no consumidor", 10*time.Second, func() bool { return count() == total })

	mu.Lock()
	got := append([]delivery(nil), delivered...)
	mu.Unlock()

	var rto time.Duration
	for i, d := range got {
		if d.seq != uint64(i+1) {
			t.Fatalf("entrega %d tem seq %d: leitura perdida ou fora de ordem (RF-45, RNF-11)", i, d.seq)
		}
		if rto == 0 && d.seq > killAt && d.at.After(crashed) {
			rto = d.at.Sub(crashed)
		}
	}
	t.Logf("RTO medido: %s (da queda do primario ate a primeira entrega de leitura nova)", rto)
	if rto <= 0 || rto > rtoLimit {
		t.Fatalf("RTO = %s, limite %s (RNF-06)", rto, rtoLimit)
	}
	if backupSrv.Role() != broker.RolePrimary {
		t.Fatalf("a reserva nao foi promovida")
	}
	if n := metricValue(t, judge.Metrics(), "failover_duration_seconds_count"); n != 1 {
		t.Fatalf("failovers no juiz = %v, esperado 1", n)
	}
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

// RF-47, CT-11: dois failovers seguidos com os clientes só no vip. O antigo
// primário volta como reserva, é ressincronizado e assume na segunda queda,
// sem perder leituras confirmadas nem quebrar a ordem.
func TestTwoFailoversInARow(t *testing.T) {
	judge, err := juiz.New(juiz.Config{
		Addr: "127.0.0.1:0", FailureTimeout: judgeFailure, CheckInterval: judgeCheck, Logger: quiet,
	})
	if err != nil {
		t.Fatalf("juiz.New: %v", err)
	}
	if err := judge.Listen(); err != nil {
		t.Fatalf("juiz Listen: %v", err)
	}
	serve(t, "juiz", judge.Serve)
	judgeStarted := time.Now()

	pAddr, rAddr := freeAddr(t), freeAddr(t)
	pRepl, rRepl := freeAddr(t), freeAddr(t)
	pDir, rDir := t.TempDir(), t.TempDir()
	cfg := func(id, addr, own, peer, dir string, role broker.Role) broker.Config {
		return broker.Config{
			Addr: addr, Role: role, BrokerID: id, DataDir: dir,
			ReplicationAddr: own, ReplicaAddr: peer, ReplicaTimeout: 500 * time.Millisecond,
			JudgeAddr: judge.Addr().String(), HeartbeatInterval: heartbeatTick, FenceTimeout: fenceTimeout,
		}
	}
	rSrv, rB := startServer(t, cfg("r", rAddr, rRepl, pRepl, rDir, broker.RoleBackup))
	pSrv, pB := startServer(t, cfg("p", pAddr, pRepl, rRepl, pDir, broker.RolePrimary))
	waitFor(t, "reserva sincronizada", 5*time.Second, pSrv.ReplicaSynced)

	proxy, err := vip.New(vip.Config{Addr: "127.0.0.1:0", Backends: []string{pAddr, rAddr}, Logger: quiet})
	if err != nil {
		t.Fatalf("vip.New: %v", err)
	}
	if err := proxy.Listen(); err != nil {
		t.Fatalf("vip Listen: %v", err)
	}
	serve(t, "vip", proxy.Serve)
	addr := proxy.Addr().String()
	time.Sleep(time.Until(judgeStarted.Add(judgeFailure + 3*judgeCheck)))

	reconnect := client.ReconnectOptions{BaseDelay: 10 * time.Millisecond, MaxDelay: 200 * time.Millisecond, DialTimeout: time.Second}
	setup, cancelSetup := context.WithTimeout(context.Background(), timeout)
	defer cancelSetup()
	consumer, err := client.ConnectWithOptions(setup, addr, client.RoleConsumer, "c1", client.Options{Reconnect: reconnect})
	if err != nil {
		t.Fatalf("consumidor via vip: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })
	if err := consumer.Subscribe(setup, topicA); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	buffer, err := storage.OpenSensorBuffer(filepath.Join(t.TempDir(), "a.buf"), storage.BufferOptions{
		MaxReadings: 1000, SensorID: "a", Logger: quiet,
	})
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	t.Cleanup(func() { _ = buffer.Close() })
	sensor, err := client.ConnectWithOptions(setup, addr, client.RoleSensor, "a", client.Options{
		Reconnect: reconnect,
		Sensor:    &client.SensorOptions{Buffer: buffer, Topic: topicA},
	})
	if err != nil {
		t.Fatalf("sensor via vip: %v", err)
	}
	t.Cleanup(func() { _ = sensor.Close() })

	var (
		mu   sync.Mutex
		seqs []uint64
	)
	dedup := client.NewDedup(0)
	go func() {
		for m := range consumer.Messages() {
			if dedup.AcceptReading(m) {
				mu.Lock()
				seqs = append(seqs, m.Seq)
				mu.Unlock()
			}
		}
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seqs)
	}
	// send só publica (pode rodar fora da goroutine do teste); drain espera
	// o buffer zerar e as entregas chegarem.
	send := func(from, to int) {
		for i := from; i <= to; i++ {
			pubCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, _, _ = sensor.PublishBuffered(pubCtx, time.Now(), 20+float32(i)/10)
			cancel()
			time.Sleep(10 * time.Millisecond)
		}
	}
	drain := func(to int) {
		waitFor(t, "buffer vazio", 15*time.Second, func() bool { return buffer.Stats().Pending == 0 })
		waitFor(t, "entregas", 15*time.Second, func() bool { return count() == to })
	}
	publish := func(from, to int) {
		send(from, to)
		drain(to)
	}

	publish(1, 20)

	// 1º failover: P cai, R assume.
	pB.stop(t)
	publish(21, 40)
	if rSrv.Role() != broker.RolePrimary {
		t.Fatalf("R nao foi promovida")
	}

	// P volta como reserva e é ressincronizado por R, sem parar o fluxo.
	pSrv2, _ := startServer(t, cfg("p", pAddr, pRepl, rRepl, pDir, broker.RolePrimary))
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		send(41, 60)
	}()
	waitFor(t, "P como reserva sincronizada", 15*time.Second, func() bool {
		return pSrv2.Role() == broker.RoleBackup && rSrv.ReplicaSynced()
	})
	<-sent
	drain(60)

	// 2º failover: R cai, P assume.
	rB.stop(t)
	publish(61, 80)
	if pSrv2.Role() != broker.RolePrimary {
		t.Fatalf("P nao foi promovido no segundo failover")
	}

	mu.Lock()
	got := append([]uint64(nil), seqs...)
	mu.Unlock()
	for i, seq := range got {
		if seq != uint64(i+1) {
			t.Fatalf("entrega %d tem seq %d: leitura perdida ou fora de ordem", i, seq)
		}
	}
	if n := metricValue(t, judge.Metrics(), "failover_duration_seconds_count"); n != 2 {
		t.Fatalf("failovers no juiz = %v, esperado 2", n)
	}
}
