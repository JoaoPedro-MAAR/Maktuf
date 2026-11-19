package broker

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func startQueueTestServer(t *testing.T, cfg Config, prepare func(*Server)) (*Server, string) {
	t.Helper()
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cfg.Addr = "127.0.0.1:0"
	srv := NewServer(cfg)
	if prepare != nil {
		prepare(srv)
	}
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("timeout aguardando shutdown")
		}
	})
	return srv, srv.Addr().String()
}

// RF-34 / DP-06: ao lotar a fila de um sensor, a sessão para de ler o socket e
// nenhum PUBACK é enviado até abrir espaço; ao liberar, tudo é confirmado em
// ordem.
func TestSensorQueueBackpressure(t *testing.T) {
	gate := make(chan struct{})
	srv, addr := startQueueTestServer(t, Config{SensorQueueLimit: 2}, func(s *Server) {
		s.scheduler.process = func(*protocol.Publish) (PublishResult, error) {
			<-gate
			return PublishResult{Status: protocol.StatusPersisted}, nil
		}
	})

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	r := bufio.NewReader(conn)
	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: protocol.RoleSensor, ClientID: "a"}); err != nil {
		t.Fatalf("CONNECT: %v", err)
	}
	if _, err := protocol.ReadMessage(r); err != nil {
		t.Fatalf("CONNACK: %v", err)
	}

	for seq := uint64(1); seq <= 5; seq++ {
		if err := protocol.WriteMessage(conn, &protocol.Publish{
			Topic: "sensores/a/temperatura", SensorID: "a", Seq: seq,
		}); err != nil {
			t.Fatalf("PUBLISH %d: %v", seq, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := srv.QueueStats()
		if stats.SensorQueues["a"] == 2 && stats.BackpressureWaits >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backpressure nao aplicado: %+v", stats)
		}
		time.Sleep(5 * time.Millisecond)
	}

	close(gate)

	for seq := uint64(1); seq <= 5; seq++ {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			t.Fatalf("PUBACK %d: %v", seq, err)
		}
		ack, ok := msg.(*protocol.PubAck)
		if !ok || ack.Seq != seq {
			t.Fatalf("PUBACK = %#v, esperado seq %d", msg, seq)
		}
	}
}
