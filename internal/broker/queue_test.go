package broker

import (
	"sync"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

func mustPush(t *testing.T, q *subscriberQueue, d *protocol.Deliver) {
	t.Helper()
	if ok, _ := q.push(d); !ok {
		t.Fatalf("leitura %s/%d nao deveria ser descartada pelo cursor", d.SensorID, d.Seq)
	}
}

// A cabeça pode estar em trânsito aguardando DELIVER_ACK e nunca é descartada.
func TestSubscriberQueueNeverDropsHead(t *testing.T) {
	q := newSubscriberQueue(2)
	a1, a2, a3 := deliver("a", 1), deliver("a", 2), deliver("a", 3)
	mustPush(t, q, a1)
	mustPush(t, q, a2)

	ok, dropped := q.push(a3)
	if !ok || len(dropped) != 1 || dropped[0] != a2 {
		t.Fatalf("descartadas = %v, esperado [a/2]", dropped)
	}
	if q.head() != a1 {
		t.Fatal("a cabeca nao pode ser descartada")
	}
	if q.len() != 2 {
		t.Fatalf("profundidade = %d, esperado 2", q.len())
	}
	if a3.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("a leitura seguinte ao descarte deveria ter GAP_BEFORE")
	}
}

// A lacuna vai para a próxima leitura do mesmo sensor, não para outro sensor.
func TestQueueGapGoesToNextReadingOfSameSensor(t *testing.T) {
	q := &readingQueue{}
	a1, b1, a2 := deliver("a", 1), deliver("b", 1), deliver("a", 2)
	q.push(a1)
	q.push(b1)
	q.push(a2)

	if got := q.dropAt(1); got != b1 {
		t.Fatalf("descartada = %v, esperado b/1", got)
	}
	if a1.Flags&protocol.DeliverFlagGap != 0 || a2.Flags&protocol.DeliverFlagGap != 0 {
		t.Fatal("lacuna de b nao pode marcar leituras de a")
	}
	b2 := deliver("b", 2)
	q.push(b2)
	if b2.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("b/2 deveria receber a lacuna pendente de b")
	}
}

// Sem leitura posterior, a lacuna fica pendente e é aplicada no próximo push.
func TestQueueGapPendingUntilNextPush(t *testing.T) {
	q := &readingQueue{}
	a1 := deliver("a", 1)
	q.push(a1)
	q.dropAt(0)

	if !q.gaps["a"] {
		t.Fatal("lacuna deveria estar pendente")
	}
	a2 := deliver("a", 2)
	q.push(a2)
	if a2.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("a proxima leitura deveria receber a lacuna pendente")
	}
	if q.gaps["a"] {
		t.Fatal("lacuna pendente deveria ser consumida")
	}
}

// Se a leitura descartada já estava marcada, a evidência é passada adiante.
func TestQueueDropPropagatesExistingGap(t *testing.T) {
	q := &readingQueue{}
	a1 := deliver("a", 1)
	a2 := deliver("a", 2)
	a2.Flags = protocol.DeliverFlagGap
	a3 := deliver("a", 3)
	q.push(a1)
	q.push(a2)
	q.push(a3)

	q.dropAt(1)
	if a3.Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("a lacuna da leitura removida deveria passar para a seguinte")
	}
}

// Uma fila recuperada acima de um limite menor é reduzida no próximo push.
func TestSubscriberQueueTrimsRestoredQueueAboveLimit(t *testing.T) {
	q := newSubscriberQueue(2)
	for seq := uint64(1); seq <= 5; seq++ {
		q.q.items = append(q.q.items, deliver("a", seq))
	}

	ok, dropped := q.push(deliver("a", 6))
	if !ok || len(dropped) != 4 {
		t.Fatalf("descartadas = %d, esperado 4", len(dropped))
	}
	if q.len() != 2 || q.head().Seq != 1 || q.q.items[1].Seq != 6 {
		t.Fatalf("fila = %v", seqs(q.q.items))
	}
	if q.q.items[1].Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("a leitura nova deveria carregar a lacuna")
	}
}

// Round-robin: um sensor com vários jobs não impede que o outro avance.
func TestSchedulerRoundRobin(t *testing.T) {
	var mu sync.Mutex
	var order []string
	s := newScheduler(func(m *protocol.Publish) (PublishResult, error) {
		mu.Lock()
		order = append(order, m.SensorID)
		mu.Unlock()
		return PublishResult{Status: protocol.StatusPersisted}, nil
	})
	s.start()
	t.Cleanup(s.close)

	a := &sensorQueue{clientID: "a", slots: make(chan struct{}, 10)}
	b := &sensorQueue{clientID: "b", slots: make(chan struct{}, 10)}
	s.register(a)
	s.register(b)

	var jobs []*publishJob
	for _, m := range []*protocol.Publish{
		{SensorID: "a", Seq: 1},
		{SensorID: "a", Seq: 2},
		{SensorID: "a", Seq: 3},
		{SensorID: "b", Seq: 1},
	} {
		job := &publishJob{msg: m, done: make(chan publishOutcome, 1)}
		jobs = append(jobs, job)
		queue := a
		if m.SensorID == "b" {
			queue = b
		}
		s.enqueue(queue, job)
	}
	for _, job := range jobs {
		<-job.done
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"a", "b", "a", "a"}
	if len(order) != len(want) {
		t.Fatalf("ordem = %v, esperado %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("ordem = %v, esperado %v", order, want)
		}
	}
}

func TestSchedulerRemoveDiscardsPendingJobs(t *testing.T) {
	s := newScheduler(func(*protocol.Publish) (PublishResult, error) {
		return PublishResult{}, nil
	})
	q := &sensorQueue{clientID: "a", slots: make(chan struct{}, 4)}
	s.register(q)
	s.enqueue(q, &publishJob{msg: &protocol.Publish{SensorID: "a", Seq: 1}, done: make(chan publishOutcome, 1)})

	s.remove(q)
	if len(q.jobs) != 0 || len(s.ready) != 0 {
		t.Fatalf("jobs = %d, ready = %d; esperado 0", len(q.jobs), len(s.ready))
	}
	if _, ok := s.queues[q]; ok {
		t.Fatal("fila deveria estar removida do scheduler")
	}
}
