package broker

import (
	"log/slog"
	"sync"
	"time"

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
//
// A fila é limitada (RF-36). Ao encher, a leitura mais antiga depois da cabeça
// é descartada (a cabeça pode estar em trânsito) e a lacuna é marcada na
// próxima leitura do mesmo sensor com GAP_BEFORE (RF-37, CE-11).
type subscriberQueue struct {
	mu     sync.Mutex
	q      readingQueue
	limit  int
	drops  uint64
	cursor cursor
	notify chan struct{}
	owner  deliveryConn
	stop   chan struct{}

	// enqueued guarda quando cada leitura entrou na fila, para medir a
	// latência até o primeiro envio. Fica limitado ao tamanho da fila e não é
	// persistido: leituras restauradas do disco não entram na métrica.
	enqueued map[deliverKey]time.Time
	onSend   func(time.Duration)
}

type deliverKey struct {
	stream string
	seq    uint64
}

func keyOf(d *protocol.Deliver) deliverKey {
	return deliverKey{stream: streamKey(d), seq: d.Seq}
}

func newSubscriberQueue(limit int) *subscriberQueue {
	if limit < 2 {
		limit = DefaultSubscriberQueueLimit
	}
	return &subscriberQueue{
		limit:    limit,
		cursor:   make(cursor),
		notify:   make(chan struct{}, 1),
		enqueued: make(map[deliverKey]time.Time),
	}
}

func (q *subscriberQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// push enfileira a leitura. Uma leitura já coberta pelo cursor foi processada
// pelo assinante e é descartada sem gerar lacuna (CE-16). Devolve as leituras
// removidas por retenção, na ordem em que saíram.
func (q *subscriberQueue) push(d *protocol.Deliver) (bool, []*protocol.Deliver) {
	q.mu.Lock()
	if q.cursor.covers(streamKey(d), d.Seq) {
		q.mu.Unlock()
		return false, nil
	}
	var dropped []*protocol.Deliver
	for q.q.len() >= q.limit && q.q.len() > 1 {
		old := q.q.dropAt(1)
		delete(q.enqueued, keyOf(old))
		dropped = append(dropped, old)
	}
	q.q.push(d)
	q.enqueued[keyOf(d)] = time.Now()
	q.drops += uint64(len(dropped))
	q.mu.Unlock()
	q.signal()
	return true, dropped
}

func (q *subscriberQueue) head() *protocol.Deliver {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.q.len() == 0 {
		return nil
	}
	return q.q.items[0]
}

// ack confirma a cabeça. O cursor só avança depois de persist; se a
// persistência falhar, a cabeça continua pendente e será reenviada.
func (q *subscriberQueue) ack(a *protocol.DeliverAck, persist func(stream string) error) (bool, error) {
	q.mu.Lock()
	if q.q.len() == 0 {
		q.mu.Unlock()
		return false, nil
	}
	h := q.q.items[0]
	if h.Topic != a.Topic || h.SensorID != a.SensorID || h.Seq != a.Seq {
		q.mu.Unlock()
		return false, nil
	}
	stream := streamKey(h)
	if err := persist(stream); err != nil {
		q.mu.Unlock()
		return false, err
	}
	q.cursor.advance(stream, h.Seq)
	q.q.items[0] = nil
	q.q.items = q.q.items[1:]
	q.mu.Unlock()
	q.signal()
	return true, nil
}

// sent registra o primeiro envio da leitura e informa a latência desde que
// ela entrou na fila. Reenvios não têm mais a chave e não são medidos.
func (q *subscriberQueue) sent(d *protocol.Deliver) {
	q.mu.Lock()
	key := keyOf(d)
	at, ok := q.enqueued[key]
	delete(q.enqueued, key)
	onSend := q.onSend
	q.mu.Unlock()
	if ok && onSend != nil {
		onSend(time.Since(at))
	}
}

func (q *subscriberQueue) restore(c cursor, rq *readingQueue) {
	q.mu.Lock()
	q.cursor = c
	clear(q.enqueued)
	if rq != nil {
		q.q = *rq.clone()
	} else {
		q.q = readingQueue{}
	}
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
	return q.q.len()
}

func (q *subscriberQueue) depthAndDrops() (int, uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.q.len(), q.drops
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
			q.sent(h)
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
// do cursor e os descartes por retenção são gravados antes de valerem.
type DeliveryManager struct {
	mu     sync.Mutex
	queues map[string]*subscriberQueue
	store  *store
	limit  int
	log    *slog.Logger
	wg     sync.WaitGroup

	// Ganchos de métricas, definidos antes de qualquer fila ser criada.
	// onSend recebe a latência entre enfileirar e enviar o DELIVER; onAck é
	// chamado a cada DELIVER_ACK aceito.
	onSend func(time.Duration)
	onAck  func()
}

// NewDeliveryManager cria um gerenciador apenas em memória.
func NewDeliveryManager() *DeliveryManager {
	return newDeliveryManager(&store{}, nil, 0)
}

func newDeliveryManager(st *store, log *slog.Logger, limit int) *DeliveryManager {
	if log == nil {
		log = slog.Default()
	}
	if limit < 2 {
		limit = DefaultSubscriberQueueLimit
	}
	return &DeliveryManager{
		queues: make(map[string]*subscriberQueue),
		store:  st,
		limit:  limit,
		log:    log,
	}
}

func (m *DeliveryManager) queue(clientID string) *subscriberQueue {
	m.mu.Lock()
	defer m.mu.Unlock()
	q, ok := m.queues[clientID]
	if !ok {
		q = newSubscriberQueue(m.limit)
		q.onSend = m.onSend
		m.queues[clientID] = q
	}
	return q
}

// Enqueue coloca a leitura na fila do cliente e informa se ela foi aceita;
// leitura já confirmada pelo cliente (coberta pelo cursor) é descartada sem
// lacuna. Cada leitura removida por retenção é persistida como descarte; se a
// gravação falhar, o erro impede o PUBACK e o sensor reenvia.
func (m *DeliveryManager) Enqueue(clientID string, d *protocol.Deliver) (bool, error) {
	accepted, dropped := m.queue(clientID).push(d)
	for _, old := range dropped {
		if err := m.store.drop(clientID, streamKey(old), old.Seq); err != nil {
			return accepted, err
		}
		m.log.Warn("leitura descartada por retencao",
			"client_id", clientID, "sensor_id", old.SensorID, "seq", old.Seq)
	}
	return accepted, nil
}

// Ack confirma a cabeça da fila do cliente e avança o cursor. ACK que não
// corresponde à cabeça (duplicado ou desconhecido) é ignorado.
func (m *DeliveryManager) Ack(clientID string, a *protocol.DeliverAck) (bool, error) {
	ok, err := m.queue(clientID).ack(a, func(stream string) error {
		return m.store.ack(clientID, stream, a.Seq)
	})
	if ok && m.onAck != nil {
		m.onAck()
	}
	return ok, err
}

func (m *DeliveryManager) restore(st *storeState) {
	for clientID, c := range st.cursors {
		m.queue(clientID).restore(c.clone(), st.queues[clientID])
	}
	for clientID, rq := range st.queues {
		if _, ok := st.cursors[clientID]; ok {
			continue
		}
		m.queue(clientID).restore(make(cursor), rq)
	}
}

func (m *DeliveryManager) Pending(clientID string) int {
	return m.queue(clientID).len()
}

func (m *DeliveryManager) stats() (depths map[string]int, drops map[string]uint64) {
	m.mu.Lock()
	queues := make(map[string]*subscriberQueue, len(m.queues))
	for clientID, q := range m.queues {
		queues[clientID] = q
	}
	m.mu.Unlock()

	depths = make(map[string]int, len(queues))
	drops = make(map[string]uint64)
	for clientID, q := range queues {
		depth, dropped := q.depthAndDrops()
		depths[clientID] = depth
		if dropped > 0 {
			drops[clientID] = dropped
		}
	}
	return depths, drops
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
