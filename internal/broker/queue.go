package broker

import "github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"

// statusStreamPrefix separa o fluxo de status do fluxo de leituras no cursor e
// nas lacunas. O status tem seq próprio e não pode avançar o cursor de leituras
// do mesmo sensor.
const statusStreamPrefix = "\x00status\x00"

func streamKey(d *protocol.Deliver) string {
	if d.Status != protocol.SensorStateUnknown {
		return statusStreamPrefix + d.SensorID
	}
	return d.SensorID
}

// readingQueue é a fila de leituras pendentes de um assinante, compartilhada
// pela memória do broker e pelo estado do store para que a recuperação
// reproduza exatamente o mesmo conteúdo. A fila é FIFO e a cabeça (items[0])
// pode estar em trânsito, aguardando DELIVER_ACK; por isso o descarte nunca
// remove a cabeça.
//
// gaps guarda, por sensor, a lacuna que ainda não pôde ser marcada em nenhuma
// leitura: houve descarte e não havia leitura posterior daquele sensor no
// instante. A próxima leitura empilhada recebe a flag GAP_BEFORE.
type readingQueue struct {
	items []*protocol.Deliver
	gaps  map[string]bool
}

func (q *readingQueue) len() int { return len(q.items) }

func (q *readingQueue) find(stream string, seq uint64) int {
	for i, d := range q.items {
		if streamKey(d) == stream && d.Seq == seq {
			return i
		}
	}
	return -1
}

// push empilha a leitura, aplicando a lacuna pendente do sensor.
func (q *readingQueue) push(d *protocol.Deliver) {
	key := streamKey(d)
	if q.gaps[key] {
		d.Flags |= protocol.DeliverFlagGap
		delete(q.gaps, key)
	}
	q.items = append(q.items, d)
}

// dropAt remove items[i] e passa a evidência de lacuna para a próxima
// leitura do mesmo sensor. Se não houver próxima, a lacuna fica pendente até o
// próximo push. A flag da própria leitura removida é passada adiante: o
// consumidor continua sabendo que houve descarte antes da leitura seguinte.
func (q *readingQueue) dropAt(i int) *protocol.Deliver {
	d := q.items[i]
	copy(q.items[i:], q.items[i+1:])
	q.items[len(q.items)-1] = nil
	q.items = q.items[:len(q.items)-1]
	q.markGap(streamKey(d), i)
	return d
}

func (q *readingQueue) markGap(stream string, from int) {
	for i := from; i < len(q.items); i++ {
		if streamKey(q.items[i]) == stream {
			q.items[i].Flags |= protocol.DeliverFlagGap
			return
		}
	}
	if q.gaps == nil {
		q.gaps = make(map[string]bool)
	}
	q.gaps[stream] = true
}

// pruneRemove remove as leituras cobertas pelo cursor sem gerar lacuna: elas
// foram confirmadas pelo assinante, não descartadas por retenção.
func (q *readingQueue) pruneRemove(covered func(*protocol.Deliver) bool) {
	kept := q.items[:0]
	for _, d := range q.items {
		if !covered(d) {
			kept = append(kept, d)
		}
	}
	for i := len(kept); i < len(q.items); i++ {
		q.items[i] = nil
	}
	q.items = kept
}

func (q *readingQueue) clone() *readingQueue {
	out := &readingQueue{items: make([]*protocol.Deliver, len(q.items))}
	for i, d := range q.items {
		cp := *d
		out.items[i] = &cp
	}
	if len(q.gaps) > 0 {
		out.gaps = make(map[string]bool, len(q.gaps))
		for sensorID := range q.gaps {
			out.gaps[sensorID] = true
		}
	}
	return out
}
