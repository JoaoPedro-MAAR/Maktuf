package broker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

type publishOutcome struct {
	res PublishResult
	err error
}

type publishJob struct {
	msg  *protocol.Publish
	done chan publishOutcome
	// received é quando o PUBLISH foi lido do socket, antes de esperar slot;
	// base da métrica ack_latency_seconds.
	received time.Time
}

// sensorQueue representa o fluxo de um sensor conectado: slots limita quantos
// PUBLISH foram aceitos e ainda não receberam PUBACK. Ao lotar, a sessão para
// de ler o socket daquele sensor (backpressure) sem afetar os demais (DP-06).
type sensorQueue struct {
	clientID string
	slots    chan struct{}
	jobs     []*publishJob
	queued   bool
}

// scheduler processa as filas dos sensores em round-robin. Uma fila só volta ao
// fim da lista depois de ceder um job, então um sensor agressivo não pode
// starvar os sensores bem-comportados (RF-38).
type scheduler struct {
	mu      sync.Mutex
	cond    *sync.Cond
	ready   []*sensorQueue
	queues  map[*sensorQueue]struct{}
	process func(*protocol.Publish) (PublishResult, error)
	closed  bool
	done    chan struct{}
	waits   atomic.Uint64
}

func newScheduler(process func(*protocol.Publish) (PublishResult, error)) *scheduler {
	s := &scheduler{
		queues:  make(map[*sensorQueue]struct{}),
		process: process,
		done:    make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *scheduler) start() {
	go s.run()
}

func (s *scheduler) run() {
	defer close(s.done)

	for {
		s.mu.Lock()
		for len(s.ready) == 0 && !s.closed {
			s.cond.Wait()
		}
		if len(s.ready) == 0 {
			s.mu.Unlock()
			return
		}

		q := s.ready[0]
		s.ready[0] = nil
		s.ready = s.ready[1:]
		q.queued = false

		if len(q.jobs) == 0 {
			s.mu.Unlock()
			continue
		}
		job := q.jobs[0]
		q.jobs[0] = nil
		q.jobs = q.jobs[1:]
		if len(q.jobs) > 0 {
			q.queued = true
			s.ready = append(s.ready, q)
		}
		s.mu.Unlock()

		res, err := s.process(job.msg)
		job.done <- publishOutcome{res: res, err: err}
	}
}

func (s *scheduler) register(q *sensorQueue) {
	s.mu.Lock()
	s.queues[q] = struct{}{}
	s.mu.Unlock()
}

func (s *scheduler) enqueue(q *sensorQueue, job *publishJob) {
	s.mu.Lock()
	q.jobs = append(q.jobs, job)
	if !q.queued {
		q.queued = true
		s.ready = append(s.ready, q)
	}
	s.mu.Unlock()
	s.cond.Signal()
}

// remove descarta os jobs ainda não iniciados de uma sessão fechada. O sensor
// reenvia as leituras na próxima conexão, pois não recebeu PUBACK.
func (s *scheduler) remove(q *sensorQueue) {
	s.mu.Lock()
	delete(s.queues, q)
	if q.queued {
		for i, r := range s.ready {
			if r == q {
				s.ready = append(s.ready[:i], s.ready[i+1:]...)
				break
			}
		}
		q.queued = false
	}
	q.jobs = nil
	s.mu.Unlock()
}

func (s *scheduler) depths() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.queues))
	for q := range s.queues {
		out[q.clientID] = len(q.slots)
	}
	return out
}

func (s *scheduler) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cond.Broadcast()
	<-s.done
}
