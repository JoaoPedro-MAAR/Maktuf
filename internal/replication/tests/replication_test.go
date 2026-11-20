package replication_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	timeout       = 3 * time.Second
	readingsTopic = "sensores/a/temperatura"
	statusTopic   = "sensores/a/status"
)

type testServer struct {
	srv      *broker.Server
	addr     string
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
}

func startBroker(t *testing.T, cfg broker.Config) *testServer {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
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

func (ts *testServer) stop(t *testing.T) {
	t.Helper()
	ts.stopOnce.Do(func() {
		ts.cancel()
		select {
		case err := <-ts.done:
			if err != nil {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout aguardando shutdown do broker")
		}
	})
}

func startBackup(t *testing.T, dataDir, replicationAddr string) *testServer {
	t.Helper()
	if replicationAddr == "" {
		replicationAddr = "127.0.0.1:0"
	}
	return startBroker(t, broker.Config{Role: broker.RoleBackup, DataDir: dataDir, ReplicationAddr: replicationAddr})
}

func startPrimary(t *testing.T, dataDir, replicaAddr string, replicaTimeout time.Duration) *testServer {
	t.Helper()
	return startBroker(t, broker.Config{DataDir: dataDir, ReplicaAddr: replicaAddr, ReplicaTimeout: replicaTimeout})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type peer struct {
	conn net.Conn
	r    *bufio.Reader
}

func dialPeer(t *testing.T, addr string, role protocol.Role, clientID string) (*peer, *protocol.ConnAck) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	p := &peer{conn: conn, r: bufio.NewReader(conn)}
	p.send(t, &protocol.Connect{Role: role, ClientID: clientID})
	ack, ok := p.read(t).(*protocol.ConnAck)
	if !ok {
		t.Fatal("esperava CONNACK")
	}
	return p, ack
}

func connectAs(t *testing.T, addr string, role protocol.Role, clientID string) *peer {
	t.Helper()
	p, ack := dialPeer(t, addr, role, clientID)
	if ack.Status != protocol.StatusOK {
		t.Fatalf("CONNACK status = %v", ack.Status)
	}
	return p
}

func (p *peer) send(t *testing.T, msg protocol.Message) {
	t.Helper()
	if err := protocol.WriteMessage(p.conn, msg); err != nil {
		t.Fatalf("envio %s: %v", msg.Type(), err)
	}
}

func (p *peer) read(t *testing.T) protocol.Message {
	t.Helper()
	_ = p.conn.SetReadDeadline(time.Now().Add(timeout))
	msg, err := protocol.ReadMessage(p.r)
	if err != nil {
		t.Fatalf("leitura: %v", err)
	}
	return msg
}

func (p *peer) expectSilence(t *testing.T) {
	t.Helper()
	_ = p.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if msg, err := protocol.ReadMessage(p.r); err == nil {
		t.Fatalf("nao esperava mensagem, obtido %s", msg.Type())
	} else {
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("esperava timeout, obtido %v", err)
		}
	}
}

func (p *peer) subscribe(t *testing.T, filter string) {
	t.Helper()
	p.send(t, &protocol.Subscribe{TopicFilter: filter})
	if ack, ok := p.read(t).(*protocol.SubAck); !ok || ack.Status != protocol.StatusOK {
		t.Fatalf("SUBACK inesperado: %#v", ack)
	}
}

func (p *peer) publish(t *testing.T, seq uint64) *protocol.PubAck {
	t.Helper()
	p.send(t, reading(seq))
	return p.readPubAck(t, seq)
}

func (p *peer) readPubAck(t *testing.T, seq uint64) *protocol.PubAck {
	t.Helper()
	ack, ok := p.read(t).(*protocol.PubAck)
	if !ok || ack.Seq != seq {
		t.Fatalf("PUBACK inesperado: %#v", ack)
	}
	return ack
}

func (p *peer) expectDeliver(t *testing.T, seq uint64) *protocol.Deliver {
	t.Helper()
	d, ok := p.read(t).(*protocol.Deliver)
	if !ok || d.Seq != seq {
		t.Fatalf("DELIVER inesperado: %#v, esperado seq %d", d, seq)
	}
	return d
}

func (p *peer) ack(t *testing.T, d *protocol.Deliver) {
	t.Helper()
	p.send(t, &protocol.DeliverAck{Topic: d.Topic, SensorID: d.SensorID, Seq: d.Seq})
}

func reading(seq uint64) *protocol.Publish {
	return &protocol.Publish{Topic: readingsTopic, SensorID: "a", Seq: seq, Temperature: 20 + float32(seq)}
}

// RF-40 / RF-41 / RF-45 / CE-08: tudo o que o primário confirmou (inscrições,
// leituras, cursor, último seq do sensor e transições de status) está no
// store da reserva. A "promoção" é simulada subindo um broker sozinho sobre o
// diretório de dados da reserva.
func TestBackupHoldsEverythingThePrimaryConfirmed(t *testing.T) {
	backupDir := t.TempDir()
	backup := startBackup(t, backupDir, "")
	primary := startPrimary(t, t.TempDir(), backup.srv.ReplicationAddr().String(), 0)
	waitFor(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	c1 := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	c1.subscribe(t, readingsTopic)
	watcher := connectAs(t, primary.addr, protocol.RoleConsumer, "watcher")
	watcher.subscribe(t, statusTopic)

	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	for seq := uint64(1); seq <= 3; seq++ {
		if ack := sensor.publish(t, seq); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v", seq, ack.Status)
		}
	}
	c1.ack(t, c1.expectDeliver(t, 1))
	// Receber a 2 prova que o DELIVER_ACK da 1 foi gravado e replicado.
	c1.expectDeliver(t, 2)

	if !primary.srv.ReplicaSynced() {
		t.Fatal("reserva deveria continuar sincronizada")
	}
	primary.stop(t)
	backup.stop(t)

	promoted := startPrimary(t, backupDir, "", 0)

	c1 = connectAs(t, promoted.addr, protocol.RoleConsumer, "c1")
	c1.ack(t, c1.expectDeliver(t, 2))
	c1.ack(t, c1.expectDeliver(t, 3))
	c1.expectSilence(t)

	watcher = connectAs(t, promoted.addr, protocol.RoleConsumer, "watcher")
	d, ok := watcher.read(t).(*protocol.Deliver)
	if !ok || d.Status != protocol.SensorStateOnline {
		t.Fatalf("transicao de status nao replicada: %#v", d)
	}

	sensor = connectAs(t, promoted.addr, protocol.RoleSensor, "a")
	if ack := sensor.publish(t, 3); ack.Status != protocol.StatusPersisted {
		t.Fatalf("reenvio: status = %v", ack.Status)
	}
	c1.expectSilence(t)
}

func TestUnsubscribeIsReplicated(t *testing.T) {
	backupDir := t.TempDir()
	backup := startBackup(t, backupDir, "")
	primary := startPrimary(t, t.TempDir(), backup.srv.ReplicationAddr().String(), 0)
	waitFor(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	c1 := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	c1.subscribe(t, readingsTopic)
	c1.send(t, &protocol.Unsubscribe{TopicFilter: readingsTopic})
	if ack, ok := c1.read(t).(*protocol.UnsubAck); !ok || ack.Status != protocol.StatusOK {
		t.Fatalf("UNSUBACK inesperado: %#v", ack)
	}
	primary.stop(t)
	backup.stop(t)

	promoted := startPrimary(t, backupDir, "", 0)
	sensor := connectAs(t, promoted.addr, protocol.RoleSensor, "a")
	if ack := sensor.publish(t, 1); ack.Status != protocol.StatusNoSubscribers {
		t.Fatalf("status = %v, esperado NO_SUBSCRIBERS", ack.Status)
	}
}

func TestBackupRejectsClients(t *testing.T) {
	backup := startBackup(t, t.TempDir(), "")
	for _, role := range []protocol.Role{protocol.RoleSensor, protocol.RoleConsumer} {
		p, ack := dialPeer(t, backup.addr, role, "x")
		if ack.Status != protocol.StatusTemporaryFailure {
			t.Fatalf("CONNACK status = %v, esperado TEMPORARY_FAILURE", ack.Status)
		}
		_ = p.conn.SetReadDeadline(time.Now().Add(timeout))
		if _, err := protocol.ReadMessage(p.r); err == nil {
			t.Fatal("a reserva deveria encerrar a conexao")
		}
	}
}

func TestReplicationConfigValidation(t *testing.T) {
	cases := []broker.Config{
		{Role: broker.RoleBackup, ReplicationAddr: "127.0.0.1:0"},
		{Role: broker.RoleBackup, DataDir: t.TempDir()},
		{ReplicaAddr: "127.0.0.1:1"},
		{Role: "juiz"},
	}
	for i, cfg := range cases {
		cfg.Addr = "127.0.0.1:0"
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		if err := broker.NewServer(cfg).Listen(); err == nil {
			t.Fatalf("caso %d: Listen deveria falhar", i)
		}
	}
}

// A reserva cai: o primário segue sozinho (PERSISTED só com o próprio disco)
// e marca a reserva como dessincronizada. Quando a reserva volta, recebe o
// estado completo, inclusive o que foi aceito sem ela.
func TestBackupDownThenResync(t *testing.T) {
	backupDir := t.TempDir()
	backup := startBackup(t, backupDir, "")
	replicationAddr := backup.srv.ReplicationAddr().String()
	primary := startPrimary(t, t.TempDir(), replicationAddr, 300*time.Millisecond)
	waitFor(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	c1 := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	c1.subscribe(t, readingsTopic)
	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	sensor.publish(t, 1)

	backup.stop(t)

	if ack := sensor.publish(t, 2); ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v, esperado PERSISTED em modo degradado", ack.Status)
	}
	waitFor(t, "reserva dessincronizada", func() bool { return !primary.srv.ReplicaSynced() })
	if n := primary.srv.ReplicaDesyncs(); n == 0 {
		t.Fatal("desync deveria ser contado")
	}
	if ack := sensor.publish(t, 3); ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v", ack.Status)
	}

	backup = startBackup(t, backupDir, replicationAddr)
	waitFor(t, "reserva ressincronizada", primary.srv.ReplicaSynced)
	primary.stop(t)
	backup.stop(t)

	promoted := startPrimary(t, backupDir, "", 0)
	c1 = connectAs(t, promoted.addr, protocol.RoleConsumer, "c1")
	for seq := uint64(1); seq <= 3; seq++ {
		c1.ack(t, c1.expectDeliver(t, seq))
	}
	c1.expectSilence(t)
}

// fakeBackup é uma reserva controlada pelo teste: sincroniza sozinha, mas cada
// REPLICA de registro fica esperando o teste liberar o REPLICA_ACK.
type fakeBackup struct {
	ln      net.Listener
	records chan *protocol.Replica
	release chan bool
	syncs   chan int
}

func startFakeBackup(t *testing.T) *fakeBackup {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBackup{
		ln:      ln,
		records: make(chan *protocol.Replica, 16),
		release: make(chan bool),
		syncs:   make(chan int, 4),
	}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve()
	return f
}

func (f *fakeBackup) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(conn)
	}
}

func (f *fakeBackup) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	r := bufio.NewReader(conn)
	syncing, count := false, 0
	for {
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			return
		}
		m := msg.(*protocol.Replica)
		switch {
		case m.Kind == protocol.ReplicaSyncStart:
			syncing, count = true, 0
			continue
		case m.Kind == protocol.ReplicaRecord && syncing:
			count++
			continue
		case m.Kind == protocol.ReplicaSyncEnd:
			syncing = false
			f.syncs <- count
		case m.Kind == protocol.ReplicaRecord:
			f.records <- m
			if ok := <-f.release; !ok {
				// Simula a reserva travada: não responde e deixa o
				// primário estourar o timeout.
				continue
			}
		}
		if err := protocol.WriteMessage(conn, &protocol.ReplicaAck{Status: protocol.StatusOK, Kind: m.Kind, Index: m.Index}); err != nil {
			return
		}
	}
}

func (f *fakeBackup) nextRecord(t *testing.T) *protocol.Replica {
	t.Helper()
	select {
	case m := <-f.records:
		return m
	case <-time.After(timeout):
		t.Fatal("primario nao enviou REPLICA")
		return nil
	}
}

func (f *fakeBackup) nextSync(t *testing.T) int {
	t.Helper()
	select {
	case n := <-f.syncs:
		return n
	case <-time.After(timeout):
		t.Fatal("primario nao sincronizou a reserva")
		return 0
	}
}

// RF-13 / RF-41 / DP-04: SUBACK, UNSUBACK e PUBACK(PERSISTED) só saem depois
// do REPLICA_ACK da reserva sincronizada.
func TestAcksWaitForReplicaAck(t *testing.T) {
	fake := startFakeBackup(t)
	primary := startPrimary(t, t.TempDir(), fake.ln.Addr().String(), timeout)
	fake.nextSync(t)
	waitFor(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	c1 := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	c1.send(t, &protocol.Subscribe{TopicFilter: readingsTopic})
	fake.nextRecord(t)
	c1.expectSilence(t)
	fake.release <- true
	if _, ok := c1.read(t).(*protocol.SubAck); !ok {
		t.Fatal("esperava SUBACK depois do REPLICA_ACK")
	}

	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	sensor.send(t, reading(1))
	if rec := fake.nextRecord(t); rec.Index == 0 || len(rec.Record) == 0 {
		t.Fatalf("REPLICA sem indice ou registro: %#v", rec)
	}
	sensor.expectSilence(t)
	c1.expectSilence(t)
	fake.release <- true
	if ack := sensor.readPubAck(t, 1); ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v", ack.Status)
	}
	c1.expectDeliver(t, 1)

	c1.send(t, &protocol.Unsubscribe{TopicFilter: readingsTopic})
	// O DELIVER_ACK pendente não foi enviado, então o próximo registro é o
	// cancelamento.
	fake.nextRecord(t)
	c1.expectSilence(t)
	fake.release <- true
	if _, ok := c1.read(t).(*protocol.UnsubAck); !ok {
		t.Fatal("esperava UNSUBACK depois do REPLICA_ACK")
	}
}

// Reserva travada: o primário espera até o timeout, segue sozinho e, quando a
// reserva aceita uma nova conexão, envia de novo o estado completo.
func TestReplicaTimeoutDesyncsAndResyncs(t *testing.T) {
	fake := startFakeBackup(t)
	primary := startPrimary(t, t.TempDir(), fake.ln.Addr().String(), 200*time.Millisecond)
	if n := fake.nextSync(t); n != 0 {
		t.Fatalf("primeira sincronizacao com %d registros, esperado 0", n)
	}
	waitFor(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	c1 := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	c1.send(t, &protocol.Subscribe{TopicFilter: readingsTopic})
	fake.nextRecord(t)
	fake.release <- true
	c1.read(t)

	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	start := time.Now()
	sensor.send(t, reading(1))
	fake.nextRecord(t)
	fake.release <- false
	if ack := sensor.readPubAck(t, 1); ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v", ack.Status)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("PUBACK em %v, antes do timeout da reserva", elapsed)
	}
	if primary.srv.ReplicaDesyncs() == 0 {
		t.Fatal("timeout deveria dessincronizar a reserva")
	}

	// Ressincronização: inscrição + leitura enfileirada + último seq.
	if n := fake.nextSync(t); n < 3 {
		t.Fatalf("ressincronizacao com %d registros, esperado o estado completo", n)
	}
	waitFor(t, "reserva ressincronizada", primary.srv.ReplicaSynced)
}
