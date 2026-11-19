package broker_test

import (
	"net"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func expectStatus(t *testing.T, c *peer, want protocol.SensorState) *protocol.Deliver {
	t.Helper()
	d := c.readDeliver(t)
	if d.Status != want {
		t.Fatalf("status = %s, esperado %s (seq %d)", d.Status, want, d.Seq)
	}
	if d.Topic != "sensores/a/status" || d.SensorID != "a" {
		t.Fatalf("entrega de status inesperada: %#v", d)
	}
	c.ackDeliver(t, d)
	return d
}

func expectSilenceFor(t *testing.T, c *peer, d time.Duration) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(d))
	msg, err := protocol.ReadMessage(c.r)
	if err == nil {
		t.Fatalf("nao esperava mensagem, obtido %s", msg.Type())
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatalf("esperava timeout, obtido %v", err)
	}
}

// RF-27 / RF-28 / CE-05 / CT-06: SLEEP marca DORMINDO e o prazo expirado
// publica OFFLINE.
func TestStatusSleepThenOffline(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{
		StatusTolerance:    50 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/status")
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")

	expectStatus(t, c, protocol.SensorStateOnline)
	sensor.send(t, &protocol.Sleep{DurationMS: 50})
	expectStatus(t, c, protocol.SensorStateDormindo)
	expectStatus(t, c, protocol.SensorStateOffline)
}

// RF-29 / CE-06: reconectar dentro do prazo publica ONLINE e cancela o OFFLINE.
func TestStatusReconnectCancelsOffline(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{
		StatusTolerance:    200 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/status")
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")
	expectStatus(t, c, protocol.SensorStateOnline)

	sensor.send(t, &protocol.Sleep{DurationMS: 200})
	expectStatus(t, c, protocol.SensorStateDormindo)
	_ = sensor.conn.Close()

	_ = connectAs(t, ts.addr, protocol.RoleSensor, "a")
	expectStatus(t, c, protocol.SensorStateOnline)

	expectSilenceFor(t, c, 700*time.Millisecond)
}

// O status também é at-least-once e sobrevive ao reinício do broker.
func TestStatusPendingSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ts := startServerWithConfig(t, broker.Config{
		DataDir:            dir,
		StatusTolerance:    50 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/status")
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")
	expectStatus(t, c, protocol.SensorStateOnline)

	sensor.send(t, &protocol.Sleep{DurationMS: 10000})
	if d := c.readDeliver(t); d.Status != protocol.SensorStateDormindo {
		t.Fatalf("status = %s, esperado dormindo", d.Status)
	}
	ts.stop(t)

	ts = startServerWithConfig(t, broker.Config{
		DataDir:            dir,
		StatusTolerance:    50 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	c = connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	expectStatus(t, c, protocol.SensorStateDormindo)
	c.expectSilence(t)
}

// Sem inscritos a transição é descartada; não há replay para quem se inscreve
// depois (mesmas regras dos tópicos de leitura).
func TestStatusWithoutSubscribersIsNotReplayed(t *testing.T) {
	ts := startServerWithConfig(t, broker.Config{
		StatusTolerance:    50 * time.Millisecond,
		StatusScanInterval: 5 * time.Millisecond,
	})
	sensor := connectAs(t, ts.addr, protocol.RoleSensor, "a")
	sensor.send(t, &protocol.Sleep{DurationMS: 10000})
	_ = sensor.conn.Close()

	c := connectAs(t, ts.addr, protocol.RoleConsumer, "c1")
	c.subscribe(t, "sensores/+/status")
	expectSilenceFor(t, c, 150*time.Millisecond)

	_ = connectAs(t, ts.addr, protocol.RoleSensor, "a")
	expectStatus(t, c, protocol.SensorStateOnline)
}
