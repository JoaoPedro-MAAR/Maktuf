package vip_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

const (
	timeout = 3 * time.Second
	topic   = "sensores/a/temperatura"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type testBroker struct {
	addr   string
	cancel context.CancelFunc
	done   chan error
}

func startBroker(t *testing.T) *testBroker {
	t.Helper()
	srv := broker.NewServer(broker.Config{Addr: "127.0.0.1:0", Logger: discard})
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &testBroker{addr: srv.Addr().String(), cancel: cancel, done: make(chan error, 1)}
	go func() { b.done <- srv.Serve(ctx) }()
	t.Cleanup(func() { b.stop(t) })
	return b
}

func (b *testBroker) stop(t *testing.T) {
	t.Helper()
	if b.cancel == nil {
		return
	}
	b.cancel()
	b.cancel = nil
	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Errorf("broker nao encerrou")
	}
}

// refusingBroker responde CONNACK(TEMPORARY_FAILURE) a todo CONNECT, como a
// reserva e o primário isolado, e conta os CONNECTs.
func refusingBroker(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	var count atomic.Int64
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if _, err := protocol.ReadMessage(bufio.NewReader(conn)); err != nil {
					return
				}
				count.Add(1)
				_ = protocol.WriteMessage(conn, &protocol.ConnAck{Status: protocol.StatusTemporaryFailure})
			}()
		}
	}()
	return ln.Addr().String(), &count
}

func startVip(t *testing.T, backends ...string) *vip.Proxy {
	t.Helper()
	p, err := vip.New(vip.Config{Addr: "127.0.0.1:0", Backends: backends, Logger: discard})
	if err != nil {
		t.Fatalf("vip.New: %v", err)
	}
	if err := p.Listen(); err != nil {
		t.Fatalf("vip Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("vip Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("vip nao encerrou")
		}
	})
	return p
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

func connectVia(t *testing.T, addr, id string, role client.Role) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c, err := client.ConnectWithOptions(ctx, addr, role, id, client.Options{
		Reconnect: client.ReconnectOptions{BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond, DialTimeout: time.Second},
	})
	if err != nil {
		t.Fatalf("Connect via vip: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestVipRoutesToTheBrokerThatAccepts(t *testing.T) {
	refusing, refused := refusingBroker(t)
	primary := startBroker(t)
	p := startVip(t, refusing, primary.addr)

	consumer := connectVia(t, p.Addr().String(), "c1", client.RoleConsumer)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := consumer.Subscribe(ctx, topic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sensor := connectVia(t, p.Addr().String(), "a", client.RoleSensor)
	status, err := sensor.Publish(ctx, topic, client.Reading{SensorID: "a", Seq: 1, Temperature: 21})
	if err != nil || status != client.StatusPersisted {
		t.Fatalf("Publish via vip = %v, %v", status, err)
	}
	select {
	case m := <-consumer.Messages():
		if m.Seq != 1 {
			t.Fatalf("seq = %d", m.Seq)
		}
	case <-time.After(timeout):
		t.Fatalf("consumidor nao recebeu pelo vip")
	}

	if got := metricValue(t, p.Metrics(), `vip_routed_total{backend="`+primary.addr+`"}`); got != 2 {
		t.Fatalf("vip_routed_total = %v, esperado 2", got)
	}
	// O primeiro CONNECT testou o broker que recusa; o segundo foi direto ao
	// primário (preferido).
	if got := refused.Load(); got != 1 {
		t.Fatalf("CONNECTs no broker que recusa = %d, esperado 1", got)
	}
}

func TestVipRejectsWhenNoBrokerAccepts(t *testing.T) {
	refusing, _ := refusingBroker(t)
	p := startVip(t, refusing, "127.0.0.1:1")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.Connect(ctx, p.Addr().String(), client.RoleSensor, "a")
	if !errors.Is(err, client.ErrHandshake) {
		t.Fatalf("Connect = %v, esperado ErrHandshake", err)
	}
	if got := metricValue(t, p.Metrics(), "vip_rejected_total"); got != 1 {
		t.Fatalf("vip_rejected_total = %v, esperado 1", got)
	}
}

func TestClientConnectionDropsWithTheBroker(t *testing.T) {
	refusing, _ := refusingBroker(t)
	primary := startBroker(t)
	p := startVip(t, primary.addr, refusing)

	c := connectVia(t, p.Addr().String(), "c1", client.RoleConsumer)
	if !c.Connected() {
		t.Fatalf("cliente deveria estar conectado")
	}
	primary.stop(t)

	deadline := time.Now().Add(timeout)
	for c.Connected() {
		if time.Now().After(deadline) {
			t.Fatalf("a conexao do cliente nao caiu junto com o broker")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestVipClosesWhenFirstFrameIsNotConnect(t *testing.T) {
	refusing, refused := refusingBroker(t)
	p := startVip(t, refusing)

	conn, err := net.Dial("tcp", p.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := protocol.WriteMessage(conn, &protocol.Publish{Topic: topic, SensorID: "a", Seq: 1}); err != nil {
		t.Fatalf("envio: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := protocol.ReadMessage(bufio.NewReader(conn)); err == nil {
		t.Fatalf("vip respondeu a um PUBLISH sem CONNECT")
	}
	if refused.Load() != 0 {
		t.Fatalf("vip contatou um broker sem CONNECT valido")
	}
}

func TestNewRequiresBackends(t *testing.T) {
	if _, err := vip.New(vip.Config{}); !errors.Is(err, vip.ErrInvalidConfig) {
		t.Fatalf("New sem backends = %v, esperado ErrInvalidConfig", err)
	}
}
