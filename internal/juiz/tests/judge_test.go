package juiz_test

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/juiz"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	failureTimeout = 150 * time.Millisecond
	checkInterval  = 20 * time.Millisecond
	waitTimeout    = 3 * time.Second
)

func startJudge(t *testing.T) string {
	t.Helper()
	j, err := juiz.New(juiz.Config{
		Addr:           "127.0.0.1:0",
		FailureTimeout: failureTimeout,
		CheckInterval:  checkInterval,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := j.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(waitTimeout):
			t.Errorf("juiz nao encerrou")
		}
	})
	return j.Addr().String()
}

// fakeBroker fala o protocolo do Juiz direto pelo codec.
type fakeBroker struct {
	id   string
	conn net.Conn
	r    *bufio.Reader
	msgs chan protocol.Message
}

func dialBroker(t *testing.T, addr, id string) *fakeBroker {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	b := &fakeBroker{id: id, conn: conn, r: bufio.NewReader(conn), msgs: make(chan protocol.Message, 64)}
	go func() {
		defer close(b.msgs)
		for {
			msg, err := protocol.ReadMessage(b.r)
			if err != nil {
				return
			}
			b.msgs <- msg
		}
	}()
	t.Cleanup(func() { _ = conn.Close() })
	return b
}

func (b *fakeBroker) heartbeat(t *testing.T, state protocol.BrokerState, index uint64) {
	t.Helper()
	if err := protocol.WriteMessage(b.conn, &protocol.Heartbeat{BrokerID: b.id, State: state, LastSeq: index}); err != nil {
		t.Fatalf("HEARTBEAT de %s: %v", b.id, err)
	}
}

// keepAlive manda heartbeats até stop fechar.
func (b *fakeBroker) keepAlive(t *testing.T, state protocol.BrokerState, index uint64, stop <-chan struct{}) {
	t.Helper()
	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		for {
			if err := protocol.WriteMessage(b.conn, &protocol.Heartbeat{BrokerID: b.id, State: state, LastSeq: index}); err != nil {
				return
			}
			select {
			case <-ticker.C:
			case <-stop:
				return
			}
		}
	}()
}

// next devolve a próxima mensagem que satisfaz match.
func (b *fakeBroker) next(t *testing.T, what string, match func(protocol.Message) bool) protocol.Message {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		select {
		case msg, ok := <-b.msgs:
			if !ok {
				t.Fatalf("%s: conexao encerrada aguardando %s", b.id, what)
			}
			if match(msg) {
				return msg
			}
		case <-deadline:
			t.Fatalf("%s: timeout aguardando %s", b.id, what)
		}
	}
}

func TestJudgeEchoesHeartbeatIndex(t *testing.T) {
	addr := startJudge(t)
	b := dialBroker(t, addr, "p")
	b.heartbeat(t, protocol.BrokerStatePrimary, 42)

	msg := b.next(t, "eco", func(m protocol.Message) bool { _, ok := m.(*protocol.Heartbeat); return ok })
	echo := msg.(*protocol.Heartbeat)
	if echo.BrokerID != protocol.JudgeID || echo.LastSeq != 42 {
		t.Fatalf("eco = %+v", echo)
	}
}

func TestJudgePromotesSyncedBackupWhenPrimaryStops(t *testing.T) {
	addr := startJudge(t)
	p := dialBroker(t, addr, "p")
	r := dialBroker(t, addr, "r")

	stopP := make(chan struct{})
	stopR := make(chan struct{})
	defer close(stopR)
	p.keepAlive(t, protocol.BrokerStatePrimary, 100, stopP)
	r.keepAlive(t, protocol.BrokerStateBackupSynced, 100, stopR)

	// Passa o período de aprendizado com o primário vivo: nada de PROMOTE.
	time.Sleep(2 * failureTimeout)
	close(stopP)

	msg := r.next(t, "PROMOTE", func(m protocol.Message) bool { _, ok := m.(*protocol.Promote); return ok })
	if got := msg.(*protocol.Promote).BrokerID; got != "r" {
		t.Fatalf("PROMOTE(%q), esperado PROMOTE(r)", got)
	}
}

func TestJudgeClosesOnlyTheInvalidConnection(t *testing.T) {
	addr := startJudge(t)
	bad := dialBroker(t, addr, "x")
	good := dialBroker(t, addr, "p")

	if err := protocol.WriteMessage(bad.conn, &protocol.Sleep{DurationMS: 1}); err != nil {
		t.Fatalf("envio: %v", err)
	}
	select {
	case _, ok := <-bad.msgs:
		if ok {
			t.Fatalf("conexao invalida recebeu mensagem")
		}
	case <-time.After(waitTimeout):
		t.Fatalf("conexao invalida nao foi encerrada")
	}

	good.heartbeat(t, protocol.BrokerStatePrimary, 1)
	good.next(t, "eco", func(m protocol.Message) bool { _, ok := m.(*protocol.Heartbeat); return ok })
}
