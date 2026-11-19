// Package e2e_test valida o caminho real usado pelas aplicações: broker em
// porta efêmera, sensor e consumidores conectados pela biblioteca cliente.
package e2e_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
)

const (
	timeout      = 2 * time.Second
	topicA       = "sensores/a/temperatura"
	topicB       = "sensores/b/temperatura"
	controlTopic = "controle/marcador"
	controlID    = "controle"
)

type testBroker struct {
	addr   string
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

// startBroker sobe o broker em 127.0.0.1:0 e devolve o endereço com a porta
// atribuída pelo sistema, sem portas fixas.
func startBroker(t *testing.T) string {
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
	go func() { done <- srv.Serve(ctx) }()

	b := &testBroker{addr: srv.Addr().String(), cancel: cancel, done: done}
	t.Cleanup(func() { b.stop(t) })
	return b.addr
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
		case <-time.After(timeout):
			t.Errorf("timeout aguardando shutdown do broker")
		}
	})
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return c
}

func connect(t *testing.T, addr string, role client.Role, clientID string) *client.Client {
	t.Helper()
	c, err := client.Connect(ctx(t), addr, role, clientID)
	if err != nil {
		t.Fatalf("Connect(%s): %v", clientID, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func subscribe(t *testing.T, c *client.Client, filter string) {
	t.Helper()
	if err := c.Subscribe(ctx(t), filter); err != nil {
		t.Fatalf("%s Subscribe(%s): %v", c.ClientID(), filter, err)
	}
}

func unsubscribe(t *testing.T, c *client.Client, filter string) {
	t.Helper()
	if err := c.Unsubscribe(ctx(t), filter); err != nil {
		t.Fatalf("%s Unsubscribe(%s): %v", c.ClientID(), filter, err)
	}
}

func publish(t *testing.T, sensor *client.Client, topic string, r client.Reading, want client.Status) {
	t.Helper()
	got, err := sensor.Publish(ctx(t), topic, r)
	if err != nil {
		t.Fatalf("Publish(%s, seq=%d): %v", topic, r.Seq, err)
	}
	if got != want {
		t.Fatalf("Publish(%s, seq=%d) = %s, esperado %s", topic, r.Seq, got, want)
	}
}

func receive(t *testing.T, c *client.Client) client.Message {
	t.Helper()
	select {
	case msg, ok := <-c.Messages():
		if !ok {
			t.Fatalf("%s: conexao encerrada: %v", c.ClientID(), c.Err())
		}
		return msg
	case <-time.After(timeout):
		t.Fatalf("%s: timeout aguardando DELIVER", c.ClientID())
	}
	return client.Message{}
}

func expect(t *testing.T, c *client.Client, topic, sensorID string, seq uint64) client.Message {
	t.Helper()
	msg := receive(t, c)
	if msg.Topic != topic || msg.SensorID != sensorID || msg.Seq != seq {
		t.Fatalf("%s recebeu %s/%s/%d, esperado %s/%s/%d",
			c.ClientID(), msg.Topic, msg.SensorID, msg.Seq, topic, sensorID, seq)
	}
	return msg
}

// expectNothingBefore publica um marcador no tópico de controle e exige que ele
// seja a próxima entrega de cada consumidor. Como a fila de cada inscrito é
// FIFO, qualquer leitura enfileirada antes do marcador chegaria primeiro.
func expectNothingBefore(t *testing.T, sensor *client.Client, seq uint64, consumers ...*client.Client) {
	t.Helper()
	publish(t, sensor, controlTopic, client.Reading{SensorID: controlID, Seq: seq}, client.StatusPersisted)
	for _, c := range consumers {
		expect(t, c, controlTopic, controlID, seq)
	}
}

// CT-01, RF-07: sem inscritos, a leitura é descartada com NO_SUBSCRIBERS,
// inclusive quando há inscritos em outros tópicos.
func TestPublishWithoutSubscribersReturnsNoSubscribers(t *testing.T) {
	addr := startBroker(t)
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")

	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 1}, client.StatusNoSubscribers)

	consumer := connect(t, addr, client.RoleConsumer, "consumidor-1")
	subscribe(t, consumer, topicB)
	subscribe(t, consumer, controlTopic)

	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 2}, client.StatusNoSubscribers)
	expectNothingBefore(t, sensor, 1, consumer)
}

// RF-02, RF-03: a leitura publicada chega ao inscrito com os dados do sensor
// e o instante calculado pelo broker a partir da idade.
func TestPublishWithSubscriberDelivers(t *testing.T) {
	addr := startBroker(t)
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")
	consumer := connect(t, addr, client.RoleConsumer, "consumidor-1")
	subscribe(t, consumer, topicA)

	before := time.Now()
	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 1, Age: 5 * time.Second, Temperature: 21.75},
		client.StatusPersisted)

	msg := expect(t, consumer, topicA, "a", 1)
	if msg.Temperature != 21.75 {
		t.Fatalf("temperatura = %v, esperado 21.75", msg.Temperature)
	}
	if want := before.Add(-5 * time.Second); msg.Timestamp.Sub(want).Abs() > time.Second {
		t.Fatalf("timestamp = %v, esperado proximo de %v", msg.Timestamp, want)
	}
}

// CT-02, RF-08: a leitura é entregue a todos os inscritos, uma única vez por
// inscrito mesmo quando mais de um filtro dele casa, e na ordem do sensor.
func TestPublishDeliversToAllSubscribers(t *testing.T) {
	addr := startBroker(t)
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")

	exact := connect(t, addr, client.RoleConsumer, "consumidor-exato")
	subscribe(t, exact, topicA)

	wildcard := connect(t, addr, client.RoleConsumer, "consumidor-curinga")
	subscribe(t, wildcard, "sensores/+/temperatura")

	both := connect(t, addr, client.RoleConsumer, "consumidor-dois-filtros")
	subscribe(t, both, topicA)
	subscribe(t, both, "sensores/+/temperatura")

	other := connect(t, addr, client.RoleConsumer, "consumidor-outro")
	subscribe(t, other, topicB)

	consumers := []*client.Client{exact, wildcard, both}
	for _, c := range append(consumers, other) {
		subscribe(t, c, controlTopic)
	}

	const readings = 5
	for seq := uint64(1); seq <= readings; seq++ {
		publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: seq}, client.StatusPersisted)
	}

	for _, c := range consumers {
		for seq := uint64(1); seq <= readings; seq++ {
			expect(t, c, topicA, "a", seq)
		}
	}
	expectNothingBefore(t, sensor, 1, append(consumers, other)...)
}

// CT-03, RF-04, RF-10: após o UNSUBSCRIBE o consumidor não recebe novas
// leituras; os demais inscritos continuam recebendo e, quando o último cancela,
// o broker volta a responder NO_SUBSCRIBERS.
func TestUnsubscribeStopsNewDeliveries(t *testing.T) {
	addr := startBroker(t)
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")

	leaving := connect(t, addr, client.RoleConsumer, "consumidor-sai")
	staying := connect(t, addr, client.RoleConsumer, "consumidor-fica")
	for _, c := range []*client.Client{leaving, staying} {
		subscribe(t, c, topicA)
		subscribe(t, c, controlTopic)
	}

	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 1}, client.StatusPersisted)
	expect(t, leaving, topicA, "a", 1)
	expect(t, staying, topicA, "a", 1)

	unsubscribe(t, leaving, topicA)

	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 2}, client.StatusPersisted)
	expect(t, staying, topicA, "a", 2)
	expectNothingBefore(t, sensor, 1, leaving, staying)

	unsubscribe(t, staying, topicA)

	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: 3}, client.StatusNoSubscribers)
	expectNothingBefore(t, sensor, 2, leaving, staying)
}

// RF-10, CE-12: leituras enfileiradas antes do UNSUBSCRIBE já foram confirmadas
// ao sensor com PERSISTED e continuam sendo entregues, em ordem; só as
// publicadas depois do cancelamento deixam de chegar.
func TestUnsubscribeKeepsAlreadyQueuedReadings(t *testing.T) {
	addr := startBroker(t)
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")
	consumer := connect(t, addr, client.RoleConsumer, "consumidor-1")
	subscribe(t, consumer, topicA)
	subscribe(t, consumer, controlTopic)

	// O consumidor não lê o canal até o fim do cancelamento. A biblioteca só
	// confirma o que coube no buffer do canal, então a maior parte destas
	// leituras fica pendente na fila do broker durante o UNSUBSCRIBE.
	const queued = 200
	for seq := uint64(1); seq <= queued; seq++ {
		publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: seq}, client.StatusPersisted)
	}

	unsubscribe(t, consumer, topicA)
	publish(t, sensor, topicA, client.Reading{SensorID: "a", Seq: queued + 1}, client.StatusNoSubscribers)

	for seq := uint64(1); seq <= queued; seq++ {
		expect(t, consumer, topicA, "a", seq)
	}
	expectNothingBefore(t, sensor, 1, consumer)
}

// Garante que a suíte não dependa de porta fixa: dois brokers sobem lado a lado.
func TestBrokersUseEphemeralPorts(t *testing.T) {
	first, second := startBroker(t), startBroker(t)
	if first == second {
		t.Fatalf("brokers no mesmo endereco %s", first)
	}
	for _, addr := range []string{first, second} {
		c := connect(t, addr, client.RoleSensor, "sensor-a")
		publish(t, c, topicA, client.Reading{SensorID: "a", Seq: 1}, client.StatusNoSubscribers)
	}
}
