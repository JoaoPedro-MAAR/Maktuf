package broker_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const sensorTopic = "sensores/a/temperatura"

type peer struct {
	conn net.Conn
	r    *bufio.Reader
}

func connectAs(t *testing.T, addr string, role protocol.Role, clientID string) *peer {
	t.Helper()
	conn := dial(t, addr)
	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: role, ClientID: clientID}); err != nil {
		t.Fatalf("envio CONNECT: %v", err)
	}
	r := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	if ack := readConnAck(t, r); ack.Status != protocol.StatusOK {
		t.Fatalf("status CONNACK = %v", ack.Status)
	}
	return &peer{conn: conn, r: r}
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

func (p *peer) subscribe(t *testing.T, filter string) {
	t.Helper()
	p.send(t, &protocol.Subscribe{TopicFilter: filter})
	if ack, ok := p.read(t).(*protocol.SubAck); !ok || ack.Status != protocol.StatusOK {
		t.Fatalf("SUBACK inesperado: %#v", ack)
	}
}

func (p *peer) publish(t *testing.T, pub *protocol.Publish) *protocol.PubAck {
	t.Helper()
	p.send(t, pub)
	msg := p.read(t)
	ack, ok := msg.(*protocol.PubAck)
	if !ok {
		t.Fatalf("esperava PUBACK, obtido %T", msg)
	}
	if ack.Seq != pub.Seq {
		t.Fatalf("PUBACK seq = %d, esperado %d", ack.Seq, pub.Seq)
	}
	return ack
}

func (p *peer) readDeliver(t *testing.T) *protocol.Deliver {
	t.Helper()
	msg := p.read(t)
	d, ok := msg.(*protocol.Deliver)
	if !ok {
		t.Fatalf("esperava DELIVER, obtido %T", msg)
	}
	return d
}

func (p *peer) ackDeliver(t *testing.T, d *protocol.Deliver) {
	t.Helper()
	p.send(t, &protocol.DeliverAck{Topic: d.Topic, SensorID: d.SensorID, Seq: d.Seq})
}

func (p *peer) expectSilence(t *testing.T) {
	t.Helper()
	_ = p.conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if msg, err := protocol.ReadMessage(p.r); err == nil {
		t.Fatalf("nao esperava mensagem, obtido %s", msg.Type())
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("esperava timeout, obtido %v", err)
	}
}

func reading(seq uint64) *protocol.Publish {
	return &protocol.Publish{Topic: sensorTopic, SensorID: "a", Seq: seq, AgeMS: 0, Temperature: 20 + float32(seq)}
}

// CT-01 / RF-07.
func TestPublishWithoutSubscribers(t *testing.T) {
	ts := startServer(t)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")

	if ack := sensor.publish(t, reading(1)); ack.Status != protocol.StatusNoSubscribers {
		t.Fatalf("status = %v, esperado NO_SUBSCRIBERS", ack.Status)
	}
}

// CT-02 / RF-08 / RF-18 / RF-19.
func TestPublishDeliversToAllSubscribers(t *testing.T) {
	ts := startServer(t)
	c1 := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c2 := connectAs(t, ts.addr, protocol.RoleConsumer, "c2")
	other := connectAs(t, ts.addr, protocol.RoleConsumer, "c3")
	c1.subscribe(t, "sensores/+/temperatura")
	c2.subscribe(t, sensorTopic)
	other.subscribe(t, "sensores/b/temperatura")

	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	pub := &protocol.Publish{Topic: sensorTopic, SensorID: "a", Seq: 7, AgeMS: 5000, Temperature: 25.5}

	before := time.Now().UnixMilli()
	ack := sensor.publish(t, pub)
	after := time.Now().UnixMilli()
	if ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v, esperado PERSISTED", ack.Status)
	}

	for _, c := range []*peer{c1, c2} {
		d := c.readDeliver(t)
		if d.Topic != pub.Topic || d.SensorID != pub.SensorID || d.Seq != pub.Seq || d.Temperature != pub.Temperature {
			t.Fatalf("DELIVER inesperado: %#v", d)
		}
		if d.TimestampMS < before-5000 || d.TimestampMS > after-5000 {
			t.Fatalf("timestamp_ms = %d fora de [%d, %d]", d.TimestampMS, before-5000, after-5000)
		}
		c.ackDeliver(t, d)
	}
	other.expectSilence(t)
}

func TestPublishSubscriberAlsoMatchesTwoFiltersOnce(t *testing.T) {
	ts := startServer(t)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/temperatura")
	c.subscribe(t, sensorTopic)

	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	sensor.publish(t, reading(1))

	c.ackDeliver(t, c.readDeliver(t))
	c.expectSilence(t)
}

func TestDeliverAckKeepsSessionAlive(t *testing.T) {
	ts := startServer(t)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")

	sensor.publish(t, reading(1))
	d := c.readDeliver(t)
	c.ackDeliver(t, d)
	c.ackDeliver(t, d)
	c.send(t, &protocol.DeliverAck{Topic: "x", SensorID: "y", Seq: 99})

	c.subscribe(t, "sensores/b/temperatura")
	sensor.publish(t, reading(2))
	if d := c.readDeliver(t); d.Seq != 2 {
		t.Fatalf("seq = %d, esperado 2", d.Seq)
	}
}

// RF-15: mesmo sensor chega em ordem de seq; a próxima leitura só é enviada
// depois do DELIVER_ACK da anterior.
func TestDeliveryPreservesOrderPerSensor(t *testing.T) {
	ts := startServer(t)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")

	const total = 20
	for seq := uint64(1); seq <= total; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v", seq, ack.Status)
		}
	}

	for seq := uint64(1); seq <= total; seq++ {
		d := c.readDeliver(t)
		if d.Seq != seq {
			t.Fatalf("seq = %d, esperado %d", d.Seq, seq)
		}
		if seq == 1 {
			c.expectSilence(t)
		}
		c.ackDeliver(t, d)
	}
}

// CE-16: reenvio de seq já aceito é confirmado sem gerar nova entrega.
func TestPublishStaleSeqIsNotRedelivered(t *testing.T) {
	ts := startServer(t)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")

	sensor.publish(t, reading(5))
	for _, seq := range []uint64{5, 3} {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v, esperado PERSISTED", seq, ack.Status)
		}
	}
	sensor.publish(t, reading(6))

	for _, want := range []uint64{5, 6} {
		d := c.readDeliver(t)
		if d.Seq != want {
			t.Fatalf("seq = %d, esperado %d", d.Seq, want)
		}
		c.ackDeliver(t, d)
	}
	c.expectSilence(t)
}

// RF-12: leitura sem DELIVER_ACK é reenviada quando o assinante reconecta, e
// leituras publicadas durante a desconexão são entregues depois.
func TestUnackedDeliverIsResentAfterReconnect(t *testing.T) {
	ts := startServer(t)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")

	sensor.publish(t, reading(1))
	if d := c.readDeliver(t); d.Seq != 1 {
		t.Fatalf("seq = %d, esperado 1", d.Seq)
	}
	_ = c.conn.Close()

	sensor.publish(t, reading(2))

	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	for _, want := range []uint64{1, 2} {
		d := c.readDeliver(t)
		if d.Seq != want {
			t.Fatalf("seq = %d, esperado %d", d.Seq, want)
		}
		c.ackDeliver(t, d)
	}
	c.expectSilence(t)
}

// RF-01: publicação inválida encerra a conexão sem PUBACK.
func TestPublishInvalidClosesConnection(t *testing.T) {
	ts := startServer(t)

	cases := map[string]*protocol.Publish{
		"topico vazio":           {Topic: "", SensorID: "a", Seq: 1},
		"topico com nivel vazio": {Topic: "sensores//t", SensorID: "a", Seq: 1},
		"topico com curinga":     {Topic: "sensores/+/temperatura", SensorID: "a", Seq: 1},
		"sensor_id vazio":        {Topic: sensorTopic, SensorID: "", Seq: 1},
	}
	for name, pub := range cases {
		t.Run(name, func(t *testing.T) {
			sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
			sensor.send(t, pub)
			_ = sensor.conn.SetReadDeadline(time.Now().Add(timeout))
			if _, err := protocol.ReadMessage(sensor.r); err == nil {
				t.Fatal("esperava que a conexao fosse fechada")
			}
		})
	}
}

func TestValidTopic(t *testing.T) {
	for _, topic := range []string{"sensores", sensorTopic} {
		if !broker.ValidTopic(topic) {
			t.Errorf("ValidTopic(%q) = false", topic)
		}
	}
	for _, topic := range []string{"", "a//b", "sensores/+/t", "+", "a/#"} {
		if broker.ValidTopic(topic) {
			t.Errorf("ValidTopic(%q) = true", topic)
		}
	}
}
