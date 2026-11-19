package broker

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

type statusRecorder struct {
	mu  sync.Mutex
	got []*protocol.Deliver
}

func (r *statusRecorder) publish(d *protocol.Deliver) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *d
	r.got = append(r.got, &cp)
	return 1, nil
}

func (r *statusRecorder) list() []*protocol.Deliver {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*protocol.Deliver(nil), r.got...)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func statusManagerForTest(rec *statusRecorder, tolerance time.Duration) *statusManager {
	return newStatusManager(tolerance, rec.publish, discardLogger())
}

func TestStatusSleepMarksDormindoAndExpiresOffline(t *testing.T) {
	rec := &statusRecorder{}
	m := statusManagerForTest(rec, 2*time.Second)
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }

	m.online("a")
	m.sleep("a", 10*time.Second)

	if state, deadline := m.stateOf("a"); state != protocol.SensorStateDormindo || !deadline.Equal(now.Add(12*time.Second)) {
		t.Fatalf("estado = %s, prazo = %v", state, deadline)
	}

	m.expire(now.Add(11 * time.Second))
	if state, _ := m.stateOf("a"); state != protocol.SensorStateDormindo {
		t.Fatalf("estado antes do prazo = %s", state)
	}

	m.expire(now.Add(12 * time.Second))
	if state, _ := m.stateOf("a"); state != protocol.SensorStateOffline {
		t.Fatalf("estado apos o prazo = %s", state)
	}

	want := []protocol.SensorState{
		protocol.SensorStateOnline,
		protocol.SensorStateDormindo,
		protocol.SensorStateOffline,
	}
	got := rec.list()
	if len(got) != len(want) {
		t.Fatalf("status publicados = %d, esperado %d", len(got), len(want))
	}
	for i, d := range got {
		if d.Status != want[i] {
			t.Fatalf("status[%d] = %s, esperado %s", i, d.Status, want[i])
		}
		if d.Topic != statusTopic("a") || d.SensorID != "a" {
			t.Fatalf("entrega inesperada: %#v", d)
		}
	}
	if !(got[0].Seq < got[1].Seq && got[1].Seq < got[2].Seq) {
		t.Fatalf("seq dos status nao e crescente: %d, %d, %d", got[0].Seq, got[1].Seq, got[2].Seq)
	}
}

func TestStatusReconnectWithinDeadlineCancelsOffline(t *testing.T) {
	rec := &statusRecorder{}
	m := statusManagerForTest(rec, time.Second)
	now := time.Unix(1000, 0)
	m.now = func() time.Time { return now }

	m.online("a")
	m.sleep("a", 10*time.Second)
	m.online("a")

	m.expire(now.Add(time.Hour))
	if state, _ := m.stateOf("a"); state != protocol.SensorStateOnline {
		t.Fatalf("estado = %s, esperado online", state)
	}

	got := rec.list()
	last := got[len(got)-1]
	if last.Status != protocol.SensorStateOnline {
		t.Fatalf("ultimo status = %s, esperado online", last.Status)
	}
}

func TestStatusSeqDoesNotAdvanceReadingCursor(t *testing.T) {
	q := newSubscriberQueue(10)
	reading := deliver("a", 5)
	status := &protocol.Deliver{
		Topic: statusTopic("a"), SensorID: "a", Seq: 5,
		Status: protocol.SensorStateOnline,
	}
	q.push(reading)
	q.push(status)

	if ok, _ := q.ack(ackFor(reading), noPersist); !ok {
		t.Fatal("ack da leitura deveria ser aceito")
	}

	var streams []string
	capture := func(stream string) error {
		streams = append(streams, stream)
		return nil
	}
	if ok, _ := q.ack(ackFor(status), capture); !ok {
		t.Fatal("ack do status com o mesmo seq da leitura deveria ser aceito")
	}
	if len(streams) != 1 || streams[0] != statusStreamPrefix+"a" {
		t.Fatalf("fluxo persistido = %v, esperado %q", streams, statusStreamPrefix+"a")
	}

	// Uma leitura de seq 6 nao depende do status de seq 5.
	if ok, _ := q.push(deliver("a", 6)); !ok {
		t.Fatal("leitura 6 deveria ser aceita")
	}
}

func TestMonitorExpiresSleepingSensor(t *testing.T) {
	rec := &statusRecorder{}
	m := statusManagerForTest(rec, 10*time.Millisecond)
	m.sleep("a", 5*time.Millisecond)

	mon := newMonitor(m, 2*time.Millisecond)
	mon.start()
	defer mon.close()

	deadline := time.Now().Add(time.Second)
	for {
		got := rec.list()
		if len(got) > 0 && got[len(got)-1].Status == protocol.SensorStateOffline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("offline nao foi publicado: %#v", got)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
