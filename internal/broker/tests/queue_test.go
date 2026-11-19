package broker_test

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func startServerWithConfig(t *testing.T, cfg broker.Config) *testServer {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	srv := broker.NewServer(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	ts := &testServer{srv: srv, addr: srv.Addr().String(), cancel: cancel, done: done}
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

// CE-10 / RF-36 / RF-37 / CE-11: consumidor lento não bloqueia os demais e o
// overflow gera lacuna explícita, persistida como descarte.
func TestSlowConsumerGetsExplicitGap(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{SubscriberQueueLimit: 3})
	slow := connectAs(t, ts.addr, protocol.RoleConsumer, "slow")
	fast := connectAs(t, ts.addr, protocol.RoleConsumer, "fast")
	for _, c := range []*peer{slow, fast} {
		c.subscribe(t, sensorTopic)
	}

	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	for seq := uint64(1); seq <= 10; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v", seq, ack.Status)
		}
		// O consumidor rápido acompanha; o lento fica para trás.
		d := fast.readDeliver(t)
		if d.Seq != seq {
			t.Fatalf("fast seq = %d, esperado %d", d.Seq, seq)
		}
		fast.ackDeliver(t, d)
	}

	if n := ts.srv.QueueStats().SubscriberQueues["slow"]; n != 3 {
		t.Fatalf("profundidade do lento = %d, esperado 3", n)
	}

	first := slow.readDeliver(t)
	if first.Seq != 1 || first.Flags&protocol.DeliverFlagGap != 0 {
		t.Fatalf("primeira entrega = seq %d flags %#x", first.Seq, first.Flags)
	}
	slow.ackDeliver(t, first)

	gap := slow.readDeliver(t)
	if gap.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatalf("leitura %d deveria ter GAP_BEFORE", gap.Seq)
	}
	slow.ackDeliver(t, gap)

	last := slow.readDeliver(t)
	if last.Flags&protocol.DeliverFlagGap != 0 {
		t.Fatalf("ultima leitura %d nao deveria ter lacuna", last.Seq)
	}
	slow.ackDeliver(t, last)

	if drops := ts.srv.QueueStats().RetentionDrops["slow"]; drops == 0 {
		t.Fatal("contador de descarte por retencao deveria ser > 0")
	}
	slow.expectSilence(t)
}

// A lacuna marcada por overflow sobrevive ao reinício do broker.
func TestSlowConsumerGapSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ts := startServerWithConfig(t, broker.Config{DataDir: dir, SubscriberQueueLimit: 3})
	slow := connectAs(t, ts.addr, protocol.RoleConsumer, "slow")
	fast := connectAs(t, ts.addr, protocol.RoleConsumer, "fast")
	for _, c := range []*peer{slow, fast} {
		c.subscribe(t, sensorTopic)
	}

	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	for seq := uint64(1); seq <= 10; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v", seq, ack.Status)
		}
		d := fast.readDeliver(t)
		fast.ackDeliver(t, d)
	}
	ts.stop(t)

	ts = startServerWithConfig(t, broker.Config{DataDir: dir, SubscriberQueueLimit: 3})
	slow = connectAs(t, ts.addr, protocol.RoleConsumer, "slow")

	first := slow.readDeliver(t)
	slow.ackDeliver(t, first)
	gap := slow.readDeliver(t)
	if gap.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatalf("apos reinicio, leitura %d deveria ter GAP_BEFORE", gap.Seq)
	}
	slow.ackDeliver(t, gap)
	last := slow.readDeliver(t)
	slow.ackDeliver(t, last)
	slow.expectSilence(t)
}

// RF-38 / RNF-05: enquanto um sensor inunda o broker, a latência de outro
// sensor bem-comportado não deve explodir.
func TestSensorLatencyStaysBoundedUnderFlood(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{DataDir: t.TempDir()})
	calm := connectAs(t, ts.addr, protocol.RoleSensor, "calm")
	aggressive := connectAs(t, ts.addr, protocol.RoleSensor, "aggressive")

	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/temperatura")

	measure := func(sensor *peer, id string, firstSeq uint64) time.Duration {
		const n = 20
		samples := make([]time.Duration, 0, n)
		for seq := firstSeq; seq < firstSeq+n; seq++ {
			start := time.Now()
			if ack := sensor.publish(t, readingOf(id, seq)); ack.Status != protocol.StatusPersisted {
				t.Fatalf("%s seq %d: status = %v", id, seq, ack.Status)
			}
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples[int(float64(len(samples))*0.95)-1]
	}

	baseline := measure(calm, "calm", 1)

	started := make(chan struct{})
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for seq := uint64(1); seq <= 2000; seq++ {
			if err := protocol.WriteMessage(aggressive.conn, readingOf("aggressive", seq)); err != nil {
				return
			}
			if seq == 100 {
				close(started)
			}
		}
	}()
	select {
	case <-started:
	case <-time.After(timeout):
		t.Fatal("sensor agressivo nao comecou a inundar o broker")
	}

	underFlood := measure(calm, "calm", 21)

	select {
	case <-floodDone:
	case <-time.After(timeout):
	}
	_ = aggressive.conn.Close()

	t.Logf("latencia p95 do sensor calmo: baseline=%v sob carga=%v", baseline, underFlood)
	if limit := 2*baseline + 250*time.Millisecond; underFlood > limit {
		t.Fatalf("latencia sob carga = %v, limite = %v", underFlood, limit)
	}
}
