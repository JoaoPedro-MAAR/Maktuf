package broker_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/juiz"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// Prazos curtos para os testes de failover. fence < failure, como recomendado.
const (
	judgeFailure  = 300 * time.Millisecond
	judgeCheck    = 20 * time.Millisecond
	heartbeatTick = 30 * time.Millisecond
	fenceTimeout  = 150 * time.Millisecond
)

type testJudge struct {
	addr   string
	cancel context.CancelFunc
	done   chan error
}

func startJudge(t *testing.T, addr string) *testJudge {
	t.Helper()
	j, err := juiz.New(juiz.Config{
		Addr:           addr,
		FailureTimeout: judgeFailure,
		CheckInterval:  judgeCheck,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("juiz.New: %v", err)
	}
	if err := j.Listen(); err != nil {
		t.Fatalf("juiz Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	tj := &testJudge{addr: j.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { tj.done <- j.Serve(ctx) }()
	t.Cleanup(func() { tj.stop(t) })
	return tj
}

func (j *testJudge) stop(t *testing.T) {
	t.Helper()
	if j.cancel == nil {
		return
	}
	j.cancel()
	j.cancel = nil
	select {
	case err := <-j.done:
		if err != nil {
			t.Errorf("juiz: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("juiz nao encerrou")
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

// startPair sobe reserva e primário apontando para o Juiz e espera a reserva
// sincronizar.
func startPair(t *testing.T, judgeAddr string) (primary, backup *testServer) {
	t.Helper()
	backup = startServerWithConfig(t, broker.Config{
		Role:              broker.RoleBackup,
		DataDir:           t.TempDir(),
		ReplicationAddr:   "127.0.0.1:0",
		BrokerID:          "r",
		JudgeAddr:         judgeAddr,
		HeartbeatInterval: heartbeatTick,
		FenceTimeout:      fenceTimeout,
	})
	primary = startServerWithConfig(t, broker.Config{
		Role:              broker.RolePrimary,
		DataDir:           t.TempDir(),
		ReplicaAddr:       backup.srv.ReplicationAddr().String(),
		ReplicaTimeout:    500 * time.Millisecond,
		BrokerID:          "p",
		JudgeAddr:         judgeAddr,
		HeartbeatInterval: heartbeatTick,
		FenceTimeout:      fenceTimeout,
	})
	waitUntil(t, "reserva sincronizada", primary.srv.ReplicaSynced)
	return primary, backup
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// tryConnect devolve o status do CONNACK.
func tryConnect(t *testing.T, addr string, role protocol.Role, clientID string) protocol.Status {
	t.Helper()
	conn := dial(t, addr)
	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: role, ClientID: clientID}); err != nil {
		t.Fatalf("CONNECT: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	return readConnAck(t, bufio.NewReader(conn)).Status
}

// publishOutcome envia o PUBLISH e devolve o PUBACK ou o erro de leitura
// (conexão encerrada pelo broker).
func publishOutcome(p *peer, pub *protocol.Publish) (*protocol.PubAck, error) {
	if err := protocol.WriteMessage(p.conn, pub); err != nil {
		return nil, err
	}
	_ = p.conn.SetReadDeadline(time.Now().Add(timeout))
	msg, err := protocol.ReadMessage(p.r)
	if err != nil {
		return nil, err
	}
	ack, ok := msg.(*protocol.PubAck)
	if !ok {
		return nil, errors.New("resposta nao e PUBACK")
	}
	return ack, nil
}

// CT-11 / RF-43 / RF-45: o primário cai, o Juiz promove a reserva e ela
// atende com as inscrições e as leituras confirmadas.
func TestJudgePromotesBackupAfterPrimaryFails(t *testing.T) {
	judge := startJudge(t, "127.0.0.1:0")
	primary, backup := startPair(t, judge.addr)
	time.Sleep(judgeFailure + 2*judgeCheck) // fim do aprendizado do Juiz

	consumer := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	consumer.subscribe(t, sensorTopic)
	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	for seq := uint64(1); seq <= 3; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: %v", seq, ack.Status)
		}
	}
	consumer.ackDeliver(t, consumer.readDeliver(t)) // só o seq 1 é confirmado
	// O DELIVER_ACK é assíncrono: espera o primário (e a réplica) gravá-lo.
	waitMetric(t, primary.srv, "messages_delivered_total", 1)

	if got := tryConnect(t, backup.addr, protocol.RoleConsumer, "x"); got != protocol.StatusTemporaryFailure {
		t.Fatalf("reserva aceitou cliente antes da promocao: %v", got)
	}

	primary.stop(t)
	waitUntil(t, "promocao da reserva", func() bool { return backup.srv.Role() == broker.RolePrimary })

	again := connectAs(t, backup.addr, protocol.RoleConsumer, "c1")
	for _, want := range []uint64{2, 3} {
		d := again.readDeliver(t)
		if d.Seq != want {
			t.Fatalf("apos failover seq = %d, esperado %d", d.Seq, want)
		}
		again.ackDeliver(t, d)
	}
	sensor2 := connectAs(t, backup.addr, protocol.RoleSensor, "a")
	if ack := sensor2.publish(t, reading(3)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("seq repetido apos failover: %v", ack.Status)
	}
	if ack := sensor2.publish(t, reading(4)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("seq 4 apos failover: %v", ack.Status)
	}
	if d := again.readDeliver(t); d.Seq != 4 {
		t.Fatalf("nova leitura seq = %d, esperado 4 (inscricao restaurada)", d.Seq)
	}
}

// CT-15 / RF-48: sem o Juiz, com a reserva sincronizada, o fluxo continua.
func TestDataFlowsWithoutJudge(t *testing.T) {
	judge := startJudge(t, "127.0.0.1:0")
	primary, _ := startPair(t, judge.addr)
	judge.stop(t)

	consumer := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	consumer.subscribe(t, sensorTopic)
	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	time.Sleep(2 * fenceTimeout)
	for seq := uint64(1); seq <= 3; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d sem juiz: %v", seq, ack.Status)
		}
		d := consumer.readDeliver(t)
		consumer.ackDeliver(t, d)
	}
}

// Dupla falha: sem Juiz e sem reserva, o primário não confirma sozinho e se
// isola; com o Juiz de volta, volta a atender.
func TestPrimaryFencesWithoutJudgeAndBackup(t *testing.T) {
	judgeAddr := freeAddr(t)
	judge := startJudge(t, judgeAddr)
	primary, backup := startPair(t, judgeAddr)
	time.Sleep(judgeFailure + 2*judgeCheck)

	// Com inscrito, cada leitura é gravada e replicada (sem inscrito,
	// NO_SUBSCRIBERS não grava nada).
	consumer := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	consumer.subscribe(t, sensorTopic)
	sensor := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	if ack := sensor.publish(t, reading(1)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("seq 1: %v", ack.Status)
	}

	judge.stop(t)
	backup.stop(t)

	ack, err := publishOutcome(sensor, reading(2))
	if err == nil {
		t.Fatalf("primario confirmou sozinho sem juiz: %+v", ack)
	}
	waitUntil(t, "isolamento", func() bool {
		return tryConnect(t, primary.addr, protocol.RoleSensor, "a") == protocol.StatusTemporaryFailure
	})

	startJudge(t, judgeAddr)
	waitUntil(t, "volta do primario", func() bool {
		return tryConnect(t, primary.addr, protocol.RoleSensor, "probe") == protocol.StatusOK
	})
	sensor2 := connectAs(t, primary.addr, protocol.RoleSensor, "a")
	if ack := sensor2.publish(t, reading(2)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("seq 2 com o aval do juiz: %v", ack.Status)
	}
}

// fakeJudge aceita uma conexão de broker e permite enviar PROMOTE.
type fakeJudge struct {
	ln    net.Listener
	conns chan net.Conn
}

func startFakeJudge(t *testing.T) *fakeJudge {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fj := &fakeJudge{ln: ln, conns: make(chan net.Conn, 4)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			fj.conns <- conn
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return fj
}

// accept espera o broker conectar e devolve a conexão e um canal com os
// estados dos HEARTBEATs recebidos.
func (fj *fakeJudge) accept(t *testing.T) (net.Conn, <-chan protocol.BrokerState) {
	t.Helper()
	var conn net.Conn
	select {
	case conn = <-fj.conns:
	case <-time.After(timeout):
		t.Fatalf("broker nao conectou ao juiz falso")
	}
	t.Cleanup(func() { _ = conn.Close() })
	states := make(chan protocol.BrokerState, 256)
	go func() {
		r := bufio.NewReader(conn)
		for {
			msg, err := protocol.ReadMessage(r)
			if err != nil {
				close(states)
				return
			}
			if hb, ok := msg.(*protocol.Heartbeat); ok {
				select {
				case states <- hb.State:
				default:
				}
			}
		}
	}()
	return conn, states
}

func waitState(t *testing.T, states <-chan protocol.BrokerState, want protocol.BrokerState) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case s, ok := <-states:
			if !ok {
				t.Fatalf("conexao com o juiz falso encerrada aguardando %s", want)
			}
			if s == want {
				return
			}
		case <-deadline:
			t.Fatalf("timeout aguardando heartbeat %s", want)
		}
	}
}

// A reserva recusa o PROMOTE enquanto o primário ainda replica nela: evita
// dois primários numa partição entre Juiz e primário.
func TestBackupRefusesPromotionWhilePrimaryIsConnected(t *testing.T) {
	fj := startFakeJudge(t)
	backup := startServerWithConfig(t, broker.Config{
		Role:              broker.RoleBackup,
		DataDir:           t.TempDir(),
		ReplicationAddr:   "127.0.0.1:0",
		BrokerID:          "r",
		JudgeAddr:         fj.ln.Addr().String(),
		HeartbeatInterval: heartbeatTick,
	})
	primary := startServerWithConfig(t, broker.Config{
		DataDir:     t.TempDir(),
		ReplicaAddr: backup.srv.ReplicationAddr().String(),
	})
	waitUntil(t, "reserva sincronizada", primary.srv.ReplicaSynced)

	conn, states := fj.accept(t)
	waitState(t, states, protocol.BrokerStateBackupSynced)
	if err := protocol.WriteMessage(conn, &protocol.Promote{BrokerID: "r"}); err != nil {
		t.Fatalf("PROMOTE: %v", err)
	}
	time.Sleep(5 * heartbeatTick)
	if backup.srv.Role() != broker.RoleBackup {
		t.Fatalf("reserva promovida com o primario conectado")
	}

	// Sem o primário, o PROMOTE é aceito. Como o Juiz real, repete a cada
	// rodada até a reserva perceber o fim da conexão de replicação.
	primary.stop(t)
	stopPromote := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatTick)
		defer ticker.Stop()
		for {
			if err := protocol.WriteMessage(conn, &protocol.Promote{BrokerID: "r"}); err != nil {
				return
			}
			select {
			case <-ticker.C:
			case <-stopPromote:
				return
			}
		}
	}()
	waitState(t, states, protocol.BrokerStatePrimary)
	close(stopPromote)
	if got := tryConnect(t, backup.addr, protocol.RoleSensor, "a"); got != protocol.StatusOK {
		t.Fatalf("reserva promovida recusou CONNECT: %v", got)
	}
}

// PROMOTE(outro id) isola o primário; PROMOTE(próprio id) o reativa.
func TestDeposedPrimaryStopsServing(t *testing.T) {
	fj := startFakeJudge(t)
	primary := startServerWithConfig(t, broker.Config{
		BrokerID:          "p",
		JudgeAddr:         fj.ln.Addr().String(),
		HeartbeatInterval: heartbeatTick,
	})
	conn, states := fj.accept(t)
	waitState(t, states, protocol.BrokerStatePrimary)

	consumer := connectAs(t, primary.addr, protocol.RoleConsumer, "c1")
	if err := protocol.WriteMessage(conn, &protocol.Promote{BrokerID: "outro"}); err != nil {
		t.Fatalf("PROMOTE: %v", err)
	}
	waitState(t, states, protocol.BrokerStateFenced)
	_ = consumer.conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := protocol.ReadMessage(consumer.r); err == nil {
		t.Fatalf("sessao do consumidor deveria ter sido encerrada")
	}
	if got := tryConnect(t, primary.addr, protocol.RoleConsumer, "c1"); got != protocol.StatusTemporaryFailure {
		t.Fatalf("primario deposto aceitou CONNECT: %v", got)
	}

	if err := protocol.WriteMessage(conn, &protocol.Promote{BrokerID: "p"}); err != nil {
		t.Fatalf("PROMOTE: %v", err)
	}
	waitState(t, states, protocol.BrokerStatePrimary)
	if got := tryConnect(t, primary.addr, protocol.RoleConsumer, "c1"); got != protocol.StatusOK {
		t.Fatalf("primario reativado recusou CONNECT: %v", got)
	}
}
