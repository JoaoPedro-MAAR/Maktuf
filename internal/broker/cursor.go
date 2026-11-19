package broker

// cursor é a posição de entrega de um assinante: o último seq confirmado por
// DELIVER_ACK de cada sensor (RF-16). Como o seq é crescente por sensor, uma
// leitura coberta pelo cursor já foi processada pelo assinante e não deve ser
// entregue de novo (CE-16).
type cursor map[string]uint64

func (c cursor) covers(sensorID string, seq uint64) bool {
	last, ok := c[sensorID]
	return ok && seq <= last
}

// advance move o cursor para seq. Nunca retrocede: devolve false se seq já
// estava coberto.
func (c cursor) advance(sensorID string, seq uint64) bool {
	if c.covers(sensorID, seq) {
		return false
	}
	c[sensorID] = seq
	return true
}

func (c cursor) clone() cursor {
	out := make(cursor, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}
