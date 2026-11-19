package broker

import (
	"log/slog"
	"sync"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// deliveryConn é o lado da sessão usado pela entrega: escrever DELIVER e
// registrar falhas de escrita.
type deliveryConn interface {
	write(msg protocol.Message) error
	deliveryFailed(err error)
}

// subscriberQueue guarda, em ordem de chegada, as leituras pendentes de um
// assinante. A entrega é stop-and-wait: a cabeça só sai da fila com o
// DELIVER_ACK da mesma leitura, o que preserva a ordem por sensor (RF-15) e faz
// a cabeça ser reenviada após reconexão (RF-12). O cursor registra até onde o
// assinante confirmou cada sensor (RF-16).
type subscriberQueue struct {
	mu     sync.Mutex
	items  []*protocol.Deliver
	cursor cursor
	notify chan struct{}
	owner  deliveryConn
	stop   chan struct{}
}

func newSubscriberQueue() *subscriberQueue {
	return &subscriberQueue{cursor: make(cursor), notify: make(chan struct{}, 1)}
}

func (q *subscriberQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// push enfileira a leitura. Uma leitura já coberta pelo cursor foi processada
// pelo assinante e é descartada (CE-16).
func (q *subscriberQueue) push(d *protocol.Deliver) bool {
	q.mu.Lock()
	if q.cursor.covers(d.SensorID, d.Seq) {
		q.mu.Unlock()
		return false
	}
	q.items = append(q.items, d)
	q.mu.Unlock()
	q.signal()
	return true
}

func (q *subscriberQueue) head() *protocol.Deliver {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

// ack confirma a cabeça. O cursor só avança depois de persist; se a
// persistência falhar, a cabeça continua pendente e será reenviada.
func (q *subscriberQueue) ack(a *protocol.DeliverAck, persist func() error) (bool, error) {
	q.mu.Lock()
	if len(q.items) == 0 {
		q.mu.Unlock()
		return false, nil
	}
	h := q.items[0]
	if h.Topic != a.Topic || h.SensorID != a.SensorID || h.Seq != a.Seq {
		q.mu.Unlock()
		return false, nil
	}
	if err := persist(); err != nil {
		q.mu.Unlock()
		return false, err
	}
	q.cursor.advance(h.SensorID, h.Seq)
	q.items[0] = nil
	q.items = q.items[1:]
	q.mu.Unlock()
	q.signal()
	return true, nil
}

func (q *subscriberQueue) restore(c cursor, items []*protocol.Deliver) {
	q.mu.Lock()
	q.cursor = c
	q.items = append(q.items[:0], items...)
	q.mu.Unlock()
	q.signal()
}

func (q *subscriberQueue) position() cursor {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.cursor.clone()
}

func (q *subscriberQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// attach associa a fila a uma nova sessão, encerrando o laço da anterior.
func (q *subscriberQueue) attach(conn deliveryConn) chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stop != nil {
		close(q.stop)
	}
	q.owner = conn
	q.stop = make(chan struct{})
	return q.stop
}

func (q *subscriberQueue) detach(conn deliveryConn) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.owner != conn {
		return
	}
	close(q.stop)
	q.owner = nil
	q.stop = nil
}

func (q *subscriberQueue) run(conn deliveryConn, stop <-chan struct{}) {
	var inflight *protocol.Deliver
	for {
		select {
		case <-stop:
			return
		default:
		}

		if h := q.head(); h != nil && h != inflight {
			if err := conn.write(h); err != nil {
				conn.deliveryFailed(err)
				return
			}
			inflight = h
		}

		select {
		case <-q.notify:
		case <-stop:
			return
		}
	}
}

// DeliveryManager mantém uma fila por assinante (clientID). As filas existem
// mesmo sem sessão ativa, pois inscrições sobrevivem à desconexão; a entrega
// retoma do cursor quando o cliente reconecta. Com store em disco, o avanço
// do cursor é gravado em disco antes de a cabeça sair da fila.
type DeliveryManager struct {
	mu     sync.Mutex
	queues map[string]*subscriberQueue
	store  *store
	wg     sync.WaitGroup
}

// NewDeliveryManager cria um gerenciador apenas em memória.
func NewDeliveryManager() *DeliveryManager {
	return newDeliveryManager(&store{})
}

func newDeliveryManager(st *store) *DeliveryManager {
	return &DeliveryManager{queues: make(map[string]*subscriberQueue), store: st}
}

func (m *DeliveryManager) queue(clientID string) *subscriberQueue {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[clientID]
	if !ok {
		q = newSubscriberQueue()
		m.queues[clientID] = q
	}
	return q
}

// Enqueue coloca a leitura na fila do cliente e informa se ela foi aceita;
// leitura já confirmada pelo cliente (coberta pelo cursor) é descartada.
func (m *DeliveryManager) Enqueue(clientID string, d *protocol.Deliver) bool {
	return m.queue(clientID).push(d)
}

// Ack confirma a cabeça da fila do cliente e avança o cursor. ACK que não
// corresponde à cabeça (duplicado ou desconhecido) é ignorado.
func (m *DeliveryManager) Ack(clientID string, a *protocol.DeliverAck) (bool, error) {
	return m.queue(clientID).ack(a, func() error {
		return m.store.ack(clientID, a.SensorID, a.Seq)
	})
}

func (m *DeliveryManager) restore(st *storeState) {
	for clientID, c := range st.cursors {
		m.queue(clientID).restore(c.clone(), st.queues[clientID])
	}
	for clientID, items := range st.queues {
		if _, ok := st.cursors[clientID]; !ok {
			m.queue(clientID).restore(make(cursor), items)
		}
	}
}

func (m *DeliveryManager) Pending(clientID string) int {
	return m.queue(clientID).len()
}

func (m *DeliveryManager) attach(clientID string, conn deliveryConn) {
	q := m.queue(clientID)
	stop := q.attach(conn)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		q.run(conn, stop)
	}()
}

func (m *DeliveryManager) detach(clientID string, conn deliveryConn) {
	m.queue(clientID).detach(conn)
}

func (m *DeliveryManager) wait() {
	m.wg.Wait()
}

func (s *session) handleDeliverAck(log *slog.Logger, m *protocol.DeliverAck) error {
	ok, err := s.server.deliveries.Ack(s.clientID, m)
	if err != nil {
		return err
	}
	if !ok {
		log.Debug("DELIVER_ACK ignorado", "topic", m.Topic, "sensor_id", m.SensorID, "seq", m.Seq)
	}
	return nil
}
