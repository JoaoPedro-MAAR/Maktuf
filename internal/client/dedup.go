package client

import "sync"

// DefaultDedupLimit é a quantidade padrão de chaves mantidas na janela de
// deduplicação.
const DefaultDedupLimit = 4096

type dedupKey struct {
	sensorID string
	seq      uint64
}

// Dedup descarta leituras repetidas por sensor_id + seq mantendo uma janela
// limitada das chaves mais recentes (RF-57, RF-14). É seguro para uso
// concorrente.
type Dedup struct {
	mu    sync.Mutex
	limit int
	seen  map[dedupKey]struct{}
	ring  []dedupKey
	head  int
	size  int
}

// NewDedup cria uma janela de deduplicação. Limit <= 0 usa DefaultDedupLimit.
func NewDedup(limit int) *Dedup {
	if limit <= 0 {
		limit = DefaultDedupLimit
	}
	return &Dedup{
		limit: limit,
		seen:  make(map[dedupKey]struct{}, limit),
		ring:  make([]dedupKey, limit),
	}
}

// Accept registra sensor_id + seq e devolve true na primeira ocorrência dentro
// da janela. Quando a janela enche, a chave mais antiga é esquecida.
func (d *Dedup) Accept(sensorID string, seq uint64) bool {
	key := dedupKey{sensorID: sensorID, seq: seq}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.seen[key]; ok {
		return false
	}
	if d.size == d.limit {
		oldest := d.ring[d.head]
		delete(d.seen, oldest)
		d.ring[d.head] = key
		d.head = (d.head + 1) % d.limit
	} else {
		index := (d.head + d.size) % d.limit
		d.ring[index] = key
		d.size++
	}
	d.seen[key] = struct{}{}
	return true
}

// AcceptReading deduplica leituras de temperatura. Transições de status usam
// um fluxo de seq separado do broker e não entram na janela de leituras.
func (d *Dedup) AcceptReading(m Message) bool {
	if m.Status != SensorStateUnknown {
		return true
	}
	return d.Accept(m.SensorID, m.Seq)
}

// Reset limpa a janela.
func (d *Dedup) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.seen)
	d.ring = make([]dedupKey, d.limit)
	d.head = 0
	d.size = 0
}
