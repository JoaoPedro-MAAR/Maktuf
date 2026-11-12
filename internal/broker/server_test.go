package broker_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const timeout = 3 * time.Second

type testServer struct {
	srv      *broker.Server
	addr     string
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
	stopErr  error
}

func startServer(t *testing.T) *testServer {
	t.Helper()

	srv := broker.NewServer(broker.Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- srv.Serve(ctx)
	}()

	ts := &testServer{srv: srv, addr: srv.Addr().String(), cancel: cancel, done: done}
	t.Cleanup(func() { ts.stop(t) })
	return ts
}

func (ts *testServer) stop(t *testing.T) {
	t.Helper()
	ts.stopOnce.Do(func() {
		ts.cancel()
		select {
		case ts.stopErr = <-ts.done:
		case <-time.After(5 * time.Second):
			ts.stopErr = errors.New("timeout aguardando shutdown do broker")
		}
	})
	if ts.stopErr != nil {
		t.Fatalf("shutdown: %v", ts.stopErr)
	}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readConnAck(t *testing.T, r *bufio.Reader) *protocol.ConnAck {
	t.Helper()
	msg, err := protocol.ReadMessage(r)
	if err != nil {
		t.Fatalf("leitura CONNACK: %v", err)
	}
	ack, ok := msg.(*protocol.ConnAck)
	if !ok {
		t.Fatalf("esperava CONNACK, obtido %T", msg)
	}
	return ack
}

func TestServerAcceptsValidConnect(t *testing.T) {
	ts := startServer(t)
	conn := dial(t, ts.addr)

	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: protocol.RoleSensor, ClientID: "sensor-1"}); err != nil {
		t.Fatalf("envio CONNECT: %v", err)
	}

	ack := readConnAck(t, bufio.NewReader(conn))
	if ack.Status != protocol.StatusOK {
		t.Fatalf("status = %v, esperado %v", ack.Status, protocol.StatusOK)
	}
}

func TestServerRejectsInvalidConnect(t *testing.T) {
	ts := startServer(t)

	cases := []struct {
		name string
		role protocol.Role
		id   string
	}{
		{name: "client_id vazio", role: protocol.RoleSensor, id: ""},
		{name: "role invalido", role: protocol.Role(99), id: "sensor-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dial(t, ts.addr)
			if err := protocol.WriteMessage(conn, &protocol.Connect{Role: tc.role, ClientID: tc.id}); err != nil {
				t.Fatalf("envio CONNECT: %v", err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(timeout))
			if _, err := protocol.ReadMessage(bufio.NewReader(conn)); err == nil {
				t.Fatal("esperava que a conexao fosse fechada")
			}
		})
	}
}

func TestServerRejectsNonConnectFirstMessage(t *testing.T) {
	ts := startServer(t)
	conn := dial(t, ts.addr)

	if err := protocol.WriteMessage(conn, &protocol.Subscribe{TopicFilter: "sensores/+/temperatura"}); err != nil {
		t.Fatalf("envio SUBSCRIBE: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := protocol.ReadMessage(bufio.NewReader(conn)); err == nil {
		t.Fatal("esperava que a conexao fosse fechada")
	}
}

func TestServerKeepsAcceptingAfterInvalidConnect(t *testing.T) {
	ts := startServer(t)

	bad := dial(t, ts.addr)
	if err := protocol.WriteMessage(bad, &protocol.Connect{Role: protocol.RoleSensor, ClientID: ""}); err != nil {
		t.Fatalf("envio CONNECT invalido: %v", err)
	}
	_ = bad.SetReadDeadline(time.Now().Add(timeout))
	_, _ = protocol.ReadMessage(bufio.NewReader(bad))
	_ = bad.Close()

	good := dial(t, ts.addr)
	if err := protocol.WriteMessage(good, &protocol.Connect{Role: protocol.RoleConsumer, ClientID: "consumer-1"}); err != nil {
		t.Fatalf("envio CONNECT valido: %v", err)
	}
	ack := readConnAck(t, bufio.NewReader(good))
	if ack.Status != protocol.StatusOK {
		t.Fatalf("status = %v, esperado %v", ack.Status, protocol.StatusOK)
	}
}

func TestServerClosesMalformedConnection(t *testing.T) {
	ts := startServer(t)
	conn := dial(t, ts.addr)

	if _, err := conn.Write([]byte{0, 0, 0, 1, 0xFF}); err != nil {
		t.Fatalf("escrita malformada: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := protocol.ReadMessage(bufio.NewReader(conn)); err == nil {
		t.Fatal("esperava que a conexao fosse fechada")
	}
}

func TestServerHandlesConcurrentConnections(t *testing.T) {
	ts := startServer(t)

	const total = 5
	errs := make(chan error, total)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", ts.addr)
			if err != nil {
				errs <- fmt.Errorf("dial: %w", err)
				return
			}
			defer func() { _ = conn.Close() }()

			connect := &protocol.Connect{Role: protocol.RoleSensor, ClientID: fmt.Sprintf("sensor-%d", i)}
			if err := protocol.WriteMessage(conn, connect); err != nil {
				errs <- fmt.Errorf("envio CONNECT: %w", err)
				return
			}

			msg, err := protocol.ReadMessage(bufio.NewReader(conn))
			if err != nil {
				errs <- fmt.Errorf("leitura CONNACK: %w", err)
				return
			}
			ack, ok := msg.(*protocol.ConnAck)
			if !ok {
				errs <- fmt.Errorf("esperava CONNACK, obtido %T", msg)
				return
			}
			if ack.Status != protocol.StatusOK {
				errs <- fmt.Errorf("status = %v, esperado %v", ack.Status, protocol.StatusOK)
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("conexao concorrente: %v", err)
	}
}

func TestServerShutdownClosesConnections(t *testing.T) {
	ts := startServer(t)
	conn := dial(t, ts.addr)

	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: protocol.RoleSensor, ClientID: "sensor-1"}); err != nil {
		t.Fatalf("envio CONNECT: %v", err)
	}
	r := bufio.NewReader(conn)
	readConnAck(t, r)

	ts.stop(t)

	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := protocol.ReadMessage(r); err == nil {
		t.Fatal("esperava que a conexao fosse fechada no shutdown")
	}
}
