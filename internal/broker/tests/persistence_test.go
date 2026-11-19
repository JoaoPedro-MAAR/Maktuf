package broker_test

import (
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func readingOf(sensorID string, seq uint64) *protocol.Publish {
	return &protocol.Publish{
		Topic:       "sensores/" + sensorID + "/temperatura",
		SensorID:    sensorID,
		Seq:         seq,
		Temperature: float32(seq),
	}
}

func expectDeliver(t *testing.T, c *peer, sensorID string, seq uint64) *protocol.Deliver {
	t.Helper()
	d := c.readDeliver(t)
	if d.SensorID != sensorID || d.Seq != seq {
		t.Fatalf("DELIVER = %s/%d, esperado %s/%d", d.SensorID, d.Seq, sensorID, seq)
	}
	return d
}

// CT-16 / RF-50 / RNF-12: inscrições, leituras pendentes, cursor e último seq
// do sensor sobrevivem ao reinício do broker.
func TestRestartRecoversSubscriptionsPendingAndCursor(t *testing.T) {
	dir := t.TempDir()

	ts := startServerWithDataDir(t, dir)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	for seq := uint64(1); seq <= 3; seq++ {
		if ack := sensor.publish(t, reading(seq)); ack.Status != protocol.StatusPersisted {
			t.Fatalf("seq %d: status = %v", seq, ack.Status)
		}
	}
	c.ackDeliver(t, expectDeliver(t, c, "a", 1))
	// Receber a 2 prova que o ACK da 1 já foi processado (stop-and-wait).
	expectDeliver(t, c, "a", 2)
	ts.stop(t)

	ts = startServerWithDataDir(t, dir)
	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.ackDeliver(t, expectDeliver(t, c, "a", 2))
	c.ackDeliver(t, expectDeliver(t, c, "a", 3))
	c.expectSilence(t)

	sensor = connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	if ack := sensor.publish(t, reading(3)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("reenvio: status = %v, esperado PERSISTED", ack.Status)
	}
	c.expectSilence(t)

	if ack := sensor.publish(t, reading(4)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("seq 4: status = %v, esperado PERSISTED (inscricao recuperada)", ack.Status)
	}
	c.ackDeliver(t, expectDeliver(t, c, "a", 4))
}

// Leitura entregue e confirmada antes do reinício não volta a ser entregue.
func TestRestartDoesNotRedeliverAckedReadings(t *testing.T) {
	dir := t.TempDir()

	ts := startServerWithDataDir(t, dir)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	sensor.publish(t, reading(1))
	sensor.publish(t, reading(2))
	c.ackDeliver(t, expectDeliver(t, c, "a", 1))
	c.ackDeliver(t, expectDeliver(t, c, "a", 2))
	// Uma ida e volta na mesma conexão garante que o último ACK foi lido.
	c.subscribe(t, sensorTopic)
	ts.stop(t)

	ts = startServerWithDataDir(t, dir)
	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.expectSilence(t)
}

func TestRestartKeepsUnsubscribe(t *testing.T) {
	dir := t.TempDir()

	ts := startServerWithDataDir(t, dir)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, sensorTopic)
	c.subscribe(t, "sensores/b/temperatura")
	c.send(t, &protocol.Unsubscribe{TopicFilter: sensorTopic})
	if ack, ok := c.read(t).(*protocol.UnsubAck); !ok || ack.Status != protocol.StatusOK {
		t.Fatalf("UNSUBACK inesperado: %#v", ack)
	}
	ts.stop(t)

	ts = startServerWithDataDir(t, dir)
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	if ack := sensor.publish(t, reading(1)); ack.Status != protocol.StatusNoSubscribers {
		t.Fatalf("status = %v, esperado NO_SUBSCRIBERS", ack.Status)
	}
	sensorB := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-b")
	if ack := sensorB.publish(t, readingOf("b", 1)); ack.Status != protocol.StatusPersisted {
		t.Fatalf("status = %v, esperado PERSISTED", ack.Status)
	}
}

// RF-15 / RF-16 / RNF-11: com leituras de dois sensores intercaladas, a
// reconexão e o reinício retomam do ponto pendente e mantêm seq crescente por
// sensor, sem reentregar o que já foi confirmado.
func TestResumeKeepsOrderPerSensorAcrossReconnectAndRestart(t *testing.T) {
	dir := t.TempDir()

	ts := startServerWithDataDir(t, dir)
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/temperatura")
	sa := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-a")
	sb := connectAs(t, ts.addr, protocol.RoleSensor, "sensor-b")
	sa.publish(t, readingOf("a", 1))
	sb.publish(t, readingOf("b", 1))
	sa.publish(t, readingOf("a", 2))
	sb.publish(t, readingOf("b", 2))
	sa.publish(t, readingOf("a", 3))

	c.ackDeliver(t, expectDeliver(t, c, "a", 1))
	c.ackDeliver(t, expectDeliver(t, c, "b", 1))
	expectDeliver(t, c, "a", 2)
	_ = c.conn.Close()

	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.ackDeliver(t, expectDeliver(t, c, "a", 2))
	expectDeliver(t, c, "b", 2)
	ts.stop(t)

	ts = startServerWithDataDir(t, dir)
	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.ackDeliver(t, expectDeliver(t, c, "b", 2))
	c.ackDeliver(t, expectDeliver(t, c, "a", 3))
	c.expectSilence(t)
}
