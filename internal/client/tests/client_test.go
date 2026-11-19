package client_test

import (
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
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
)

const (
	timeout     = 3 * time.Second
	sensorTopic = "sensores/a/temperatura"
)

type testBroker struct {
	addr   string
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func startBroker(t *testing.T) *testBroker {
	t.Helper()
	return startBrokerWithConfig(t, broker.Config{})
}

func startBrokerWithConfig(t *testing.T, cfg broker.Config) *testBroker {
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

	b := &testBroker{addr: srv.Addr().String(), cancel: cancel, done: done}
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

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

func connect(t *testing.T, addr string, role client.Role, clientID string) *client.Client {
	t.Helper()
	c, err := client.Connect(testContext(t), addr, role, clientID)
	if err != nil {
		t.Fatalf("Connect(%s): %v", clientID, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func publish(t *testing.T, c *client.Client, topic string, r client.Reading) client.Status {
	t.Helper()
	status, err := c.Publish(testContext(t), topic, r)
	if err != nil {
		t.Fatalf("Publish seq=%d: %v", r.Seq, err)
	}
	return status
}

func receive(t *testing.T, c *client.Client) client.Message {
	t.Helper()
	select {
	case msg, ok := <-c.Messages():
		if !ok {
			t.Fatalf("canal de mensagens fechado: %v", c.Err())
		}
		return msg
	case <-time.After(timeout):
		t.Fatalf("timeout aguardando DELIVER")
	}
	return client.Message{}
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

func TestConnectFailsWhenBrokerIsDown(t *testing.T) {
	addr := freeAddr(t)

	c, err := client.Connect(testContext(t), addr, client.RoleSensor, "sensor-a")
	if err == nil {
		_ = c.Close()
		t.Fatalf("Connect deveria falhar com o broker fora")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("erro deveria expor a falha de rede, obtido: %v", err)
	}
}

func TestConnectFailsWhenBrokerDoesNotAnswer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer func() { _ = conn.Close() }()
			time.Sleep(time.Second)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = client.Connect(ctx, ln.Addr().String(), client.RoleSensor, "sensor-a")
	if !errors.Is(err, client.ErrHandshake) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("esperava ErrHandshake por timeout, obtido: %v", err)
	}
}

func TestConnectRejectsInvalidArguments(t *testing.T) {
	b := startBroker(t)

	if _, err := client.Connect(testContext(t), b.addr, client.RoleSensor, ""); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("client_id vazio: obtido %v", err)
	}
	if _, err := client.Connect(testContext(t), b.addr, client.Role(9), "x"); !errors.Is(err, client.ErrInvalidArgument) {
		t.Fatalf("papel invalido: obtido %v", err)
	}
}

func TestPublishReturnsBrokerStatus(t *testing.T) {
	b := startBroker(t)
	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	reading := client.Reading{SensorID: "sensor-a", Seq: 1, Age: 2 * time.Second, Temperature: 25.5}
	if got := publish(t, sensor, sensorTopic, reading); got != client.StatusNoSubscribers {
		t.Fatalf("sem inscritos: status = %s, esperado NO_SUBSCRIBERS", got)
	}

	if err := consumer.Subscribe(testContext(t), "sensores/+/temperatura"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	before := time.Now()
	reading.Seq = 2
	if got := publish(t, sensor, sensorTopic, reading); got != client.StatusPersisted {
		t.Fatalf("com inscrito: status = %s, esperado PERSISTED", got)
	}

	msg := receive(t, consumer)
	if msg.Topic != sensorTopic || msg.SensorID != "sensor-a" || msg.Seq != 2 || msg.Temperature != 25.5 {
		t.Fatalf("mensagem inesperada: %+v", msg)
	}
	if msg.Timestamp.Location() != time.UTC {
		t.Fatalf("timestamp deveria estar em UTC: %v", msg.Timestamp)
	}
	wantTS := before.Add(-2 * time.Second)
	if d := msg.Timestamp.Sub(wantTS); d < -time.Second || d > time.Second {
		t.Fatalf("timestamp = %v, esperado proximo de %v", msg.Timestamp, wantTS)
	}
}

func TestPublishRejectsInvalidReading(t *testing.T) {
	b := startBroker(t)
	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")

	cases := []client.Reading{
		{SensorID: "", Seq: 1},
		{SensorID: "sensor-a", Seq: 1, Age: -time.Second},
	}
	for _, r := range cases {
		if _, err := sensor.Publish(testContext(t), sensorTopic, r); !errors.Is(err, client.ErrInvalidArgument) {
			t.Errorf("Publish(%+v): esperava ErrInvalidArgument, obtido %v", r, err)
		}
	}
	if got := publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 1}); got != client.StatusNoSubscribers {
		t.Fatalf("conexao deveria continuar valida, status = %s", got)
	}
}

func TestDeliveryAcksAndContinuesInOrder(t *testing.T) {
	b := startBroker(t)
	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	if err := consumer.Subscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	const total = 20
	for seq := uint64(1); seq <= total; seq++ {
		r := client.Reading{SensorID: "sensor-a", Seq: seq, Temperature: float32(seq)}
		if got := publish(t, sensor, sensorTopic, r); got != client.StatusPersisted {
			t.Fatalf("seq %d: status = %s", seq, got)
		}
	}

	// O broker só envia a próxima leitura após o DELIVER_ACK da atual; receber
	// todas confirma que a biblioteca responde aos DELIVERs.
	for seq := uint64(1); seq <= total; seq++ {
		if msg := receive(t, consumer); msg.Seq != seq {
			t.Fatalf("seq recebido = %d, esperado %d", msg.Seq, seq)
		}
	}
}

func TestUnsubscribeStopsNewDeliveries(t *testing.T) {
	b := startBroker(t)
	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	if err := consumer.Subscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 1})
	if msg := receive(t, consumer); msg.Seq != 1 {
		t.Fatalf("seq = %d, esperado 1", msg.Seq)
	}

	if err := consumer.Unsubscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if got := publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 2}); got != client.StatusNoSubscribers {
		t.Fatalf("apos Unsubscribe: status = %s, esperado NO_SUBSCRIBERS", got)
	}

	select {
	case msg := <-consumer.Messages():
		t.Fatalf("nao deveria receber apos Unsubscribe: %+v", msg)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestConcurrentCallsShareConnection(t *testing.T) {
	b := startBroker(t)
	sensor := connect(t, b.addr, client.RoleSensor, "sensores")
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	const sensors = 8
	const perSensor = 25

	var wg sync.WaitGroup
	errs := make(chan error, sensors*(perSensor+2))
	for i := 0; i < sensors; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := consumer.Subscribe(testContext(t), fmt.Sprintf("sensores/s%d/temperatura", i)); err != nil {
				errs <- err
			}
			if err := consumer.Unsubscribe(testContext(t), fmt.Sprintf("extra/s%d", i)); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < sensors; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("s%d", i)
			topic := fmt.Sprintf("sensores/%s/temperatura", id)
			for seq := uint64(1); seq <= perSensor; seq++ {
				status, err := sensor.Publish(testContext(t), topic, client.Reading{SensorID: id, Seq: seq})
				if err != nil {
					errs <- err
					return
				}
				if status != client.StatusPersisted {
					errs <- fmt.Errorf("%s seq %d: status %s", id, seq, status)
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("chamada concorrente falhou: %v", err)
	}

	last := make(map[string]uint64)
	for n := 0; n < sensors*perSensor; n++ {
		msg := receive(t, consumer)
		if msg.Seq != last[msg.SensorID]+1 {
			t.Fatalf("%s: seq %d apos %d", msg.SensorID, msg.Seq, last[msg.SensorID])
		}
		last[msg.SensorID] = msg.Seq
	}
	if sensor.Err() != nil || consumer.Err() != nil {
		t.Fatalf("conexao encerrada: sensor=%v consumidor=%v", sensor.Err(), consumer.Err())
	}
}

func TestCloseEndsClient(t *testing.T) {
	b := startBroker(t)
	c := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close repetido: %v", err)
	}

	if _, ok := <-c.Messages(); ok {
		t.Fatalf("canal de mensagens deveria estar fechado")
	}
	if err := c.Subscribe(testContext(t), sensorTopic); !errors.Is(err, client.ErrClosed) {
		t.Fatalf("Subscribe apos Close: esperava ErrClosed, obtido %v", err)
	}
	if !errors.Is(c.Err(), client.ErrClosed) {
		t.Fatalf("Err = %v, esperado ErrClosed", c.Err())
	}
}

func TestBrokerShutdownIsReported(t *testing.T) {
	b := startBroker(t)
	c := connect(t, b.addr, client.RoleSensor, "sensor-a")

	b.stop(t)

	select {
	case <-c.Done():
	case <-time.After(timeout):
		t.Fatalf("Done nao foi fechado apos queda do broker")
	}
	if !errors.Is(c.Err(), client.ErrDisconnected) {
		t.Fatalf("Err = %v, esperado ErrDisconnected", c.Err())
	}
	if _, err := c.Publish(testContext(t), sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 1}); !errors.Is(err, client.ErrDisconnected) {
		t.Fatalf("Publish apos queda: esperava ErrDisconnected, obtido %v", err)
	}
}

// RF-37 / CE-11: a lacuna marcada pelo broker chega como Message.Gap.
func TestMessageGapIsExposed(t *testing.T) {
	b := startBrokerWithConfig(t, broker.Config{SubscriberQueueLimit: 2})
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")
	if err := consumer.Subscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = consumer.Close()

	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")
	for seq := uint64(1); seq <= 10; seq++ {
		if got := publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: seq}); got != client.StatusPersisted {
			t.Fatalf("seq %d: status = %s", seq, got)
		}
	}

	consumer = connect(t, b.addr, client.RoleConsumer, "consumidor-1")
	first := receive(t, consumer)
	if first.Seq != 1 || first.Gap {
		t.Fatalf("primeira mensagem = seq %d gap=%v", first.Seq, first.Gap)
	}
	last := receive(t, consumer)
	if last.Seq != 10 || !last.Gap {
		t.Fatalf("ultima mensagem = seq %d gap=%v, esperado seq 10 com lacuna", last.Seq, last.Gap)
	}
}

// RF-21 / RF-27 / RF-28: o cliente entrega online, dormindo e offline com o
// estado preenchido e envia SLEEP pelo método Sleep.
func TestStatusLifecycleExposedByClient(t *testing.T) {
	b := startBrokerWithConfig(t, broker.Config{
		StatusTolerance:    50 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	consumer := connect(t, b.addr, client.RoleConsumer, "consumidor-1")
	if err := consumer.Subscribe(testContext(t), "sensores/+/status"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sensor := connect(t, b.addr, client.RoleSensor, "sensor-a")

	if msg := receive(t, consumer); msg.Status != client.SensorStateOnline {
		t.Fatalf("status = %s, esperado online", msg.Status)
	}
	if err := sensor.Sleep(testContext(t), 50*time.Millisecond); err != nil {
		t.Fatalf("Sleep: %v", err)
	}
	if msg := receive(t, consumer); msg.Status != client.SensorStateDormindo {
		t.Fatalf("status = %s, esperado dormindo", msg.Status)
	}
	if msg := receive(t, consumer); msg.Status != client.SensorStateOffline {
		t.Fatalf("status = %s, esperado offline", msg.Status)
	}
}

func TestInvalidFilterFailsPendingCall(t *testing.T) {
	b := startBroker(t)
	c := connect(t, b.addr, client.RoleConsumer, "consumidor-1")

	// O broker encerra a conexão sem responder a filtro inválido.
	if err := c.Subscribe(testContext(t), "a//b"); !errors.Is(err, client.ErrDisconnected) {
		t.Fatalf("esperava ErrDisconnected, obtido %v", err)
	}
}
