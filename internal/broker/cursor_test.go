package broker

import (
	"errors"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func TestCursorAdvanceIsMonotonicPerSensor(t *testing.T) {
	c := make(cursor)
	if c.covers("a", 0) || c.covers("a", 1) {
		t.Fatal("cursor vazio nao deve cobrir nenhum seq")
	}
	if !c.advance("a", 5) {
		t.Fatal("advance(a, 5) deveria avancar")
	}
	if c.advance("a", 5) || c.advance("a", 3) {
		t.Fatal("cursor nao pode retroceder nem repetir")
	}
	if !c.covers("a", 5) || !c.covers("a", 1) || c.covers("a", 6) {
		t.Fatalf("covers inconsistente: %v", c)
	}
	if c.covers("b", 1) {
		t.Fatal("cursor de um sensor nao pode afetar outro")
	}
}

func deliver(sensorID string, seq uint64) *protocol.Deliver {
	return &protocol.Deliver{Topic: "sensores/" + sensorID + "/temperatura", SensorID: sensorID, Seq: seq}
}

func ackFor(d *protocol.Deliver) *protocol.DeliverAck {
	return &protocol.DeliverAck{Topic: d.Topic, SensorID: d.SensorID, Seq: d.Seq}
}

func noPersist() error { return nil }

// O cursor só avança com o DELIVER_ACK da cabeça.
func TestQueueCursorAdvancesOnlyOnHeadAck(t *testing.T) {
	q := newSubscriberQueue()
	d1, d2 := deliver("a", 1), deliver("a", 2)
	q.push(d1)
	q.push(d2)

	if _, ok := q.position()["a"]; ok {
		t.Fatal("cursor nao deve avancar antes do DELIVER_ACK")
	}
	if ok, _ := q.ack(ackFor(d2), noPersist); ok {
		t.Fatal("ACK fora da cabeca deveria ser ignorado")
	}
	if _, ok := q.position()["a"]; ok {
		t.Fatal("ACK ignorado nao pode avancar o cursor")
	}
	if ok, _ := q.ack(ackFor(d1), noPersist); !ok {
		t.Fatal("ACK da cabeca deveria ser aceito")
	}
	if got := q.position()["a"]; got != 1 {
		t.Fatalf("cursor = %d, esperado 1", got)
	}
}

func TestQueueAckKeepsHeadWhenPersistFails(t *testing.T) {
	q := newSubscriberQueue()
	d1 := deliver("a", 1)
	q.push(d1)

	boom := errors.New("disco cheio")
	if ok, err := q.ack(ackFor(d1), func() error { return boom }); ok || !errors.Is(err, boom) {
		t.Fatalf("ack = %v, %v; esperado false, %v", ok, err, boom)
	}
	if q.head() != d1 {
		t.Fatal("a cabeca deve continuar pendente para ser reenviada")
	}
	if _, ok := q.position()["a"]; ok {
		t.Fatal("cursor nao pode avancar sem persistencia")
	}
}

// CE-16: leitura já confirmada não volta para a fila.
func TestQueueDiscardsReadingsCoveredByCursor(t *testing.T) {
	q := newSubscriberQueue()
	d2 := deliver("a", 2)
	q.push(d2)
	if ok, err := q.ack(ackFor(d2), noPersist); !ok || err != nil {
		t.Fatalf("ack = %v, %v", ok, err)
	}

	if q.push(deliver("a", 1)) || q.push(deliver("a", 2)) {
		t.Fatal("leitura coberta pelo cursor nao deveria ser enfileirada")
	}
	if !q.push(deliver("a", 3)) || !q.push(deliver("b", 1)) {
		t.Fatal("leituras novas deveriam ser enfileiradas")
	}
	if q.len() != 2 {
		t.Fatalf("pendentes = %d, esperado 2", q.len())
	}
}
