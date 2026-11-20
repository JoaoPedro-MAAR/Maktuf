package broker

import (
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// brokerMetrics reúne as métricas da seção 9 de requisitos.md que o broker
// atualiza no caminho dos dados. As que já existem em outro estado (filas,
// tópicos, sensores, réplica) são lidas na hora do scrape.
type brokerMetrics struct {
	published       *metrics.Counter
	duplicated      *metrics.Counter
	noSubscribers   *metrics.Counter
	delivered       *metrics.Counter
	ackLatency      *metrics.Histogram
	deliveryLatency *metrics.Histogram
}

func newBrokerMetrics(reg *metrics.Registry, s *Server) *brokerMetrics {
	m := &brokerMetrics{
		published: reg.NewCounter("messages_published_total",
			"Leituras novas recebidas (PERSISTED ou NO_SUBSCRIBERS), por sensor e topico.", "sensor_id", "topic"),
		duplicated: reg.NewCounter("messages_duplicated_total",
			"PUBLISH com seq ja aceito, confirmado sem reprocessar (CE-16).", "sensor_id"),
		noSubscribers: reg.NewCounter("messages_dropped_no_subscribers_total",
			"Leituras descartadas por ausencia de inscritos (RF-07).", "sensor_id"),
		delivered: reg.NewCounter("messages_delivered_total",
			"Entregas confirmadas por DELIVER_ACK."),
		ackLatency: reg.NewHistogram("ack_latency_seconds",
			"Tempo entre ler o PUBLISH e escrever o PUBACK (inclui backpressure, disco e replica).", nil),
		deliveryLatency: reg.NewHistogram("delivery_latency_seconds",
			"Tempo entre enfileirar a leitura para o assinante e escrever o DELIVER.", nil),
	}

	reg.NewGaugeFunc("broker_queue_depth",
		"Profundidade das filas: PUBLISH sem PUBACK por sensor e leituras pendentes por assinante.",
		[]string{"queue", "id"}, func() []metrics.Sample {
			qs := s.QueueStats()
			out := make([]metrics.Sample, 0, len(qs.SensorQueues)+len(qs.SubscriberQueues))
			for id, depth := range qs.SensorQueues {
				out = append(out, sample(float64(depth), "queue", "sensor", "id", id))
			}
			for id, depth := range qs.SubscriberQueues {
				out = append(out, sample(float64(depth), "queue", "subscriber", "id", id))
			}
			return out
		})
	reg.NewCounterFunc("retention_drops_total",
		"Leituras descartadas por exceder a retencao do assinante (RF-37).",
		[]string{"client_id"}, func() []metrics.Sample {
			drops := s.QueueStats().RetentionDrops
			out := make([]metrics.Sample, 0, len(drops))
			for id, n := range drops {
				out = append(out, sample(float64(n), "client_id", id))
			}
			return out
		})
	reg.NewCounterFunc("backpressure_waits_total",
		"Vezes em que uma sessao de sensor parou de ler o socket por fila cheia.",
		nil, func() []metrics.Sample {
			return []metrics.Sample{{Value: float64(s.scheduler.waits.Load())}}
		})
	reg.NewGaugeFunc("topic_count", "Topicos ativos.", nil, func() []metrics.Sample {
		return []metrics.Sample{{Value: float64(s.topics.TopicCount())}}
	})
	reg.NewGaugeFunc("active_subscriptions", "Inscricoes ativas por topico.",
		[]string{"topic"}, func() []metrics.Sample {
			counts := s.topics.SubscriptionCounts()
			out := make([]metrics.Sample, 0, len(counts))
			for topic, n := range counts {
				out = append(out, sample(float64(n), "topic", topic))
			}
			return out
		})
	reg.NewGaugeFunc("active_sensors", "Sensores em estado conhecido, por estado.",
		[]string{"state"}, func() []metrics.Sample {
			counts := s.status.counts()
			states := []protocol.SensorState{protocol.SensorStateOnline, protocol.SensorStateDormindo, protocol.SensorStateOffline}
			out := make([]metrics.Sample, 0, len(states))
			for _, st := range states {
				out = append(out, sample(float64(counts[st]), "state", st.String()))
			}
			return out
		})
	reg.NewGaugeFunc("replica_synced", "1 quando a reserva esta sincronizada com este primario.",
		nil, func() []metrics.Sample {
			v := 0.0
			if s.ReplicaSynced() {
				v = 1
			}
			return []metrics.Sample{{Value: v}}
		})
	reg.NewCounterFunc("replica_desyncs_total", "Vezes em que a reserva deixou de estar sincronizada.",
		nil, func() []metrics.Sample {
			return []metrics.Sample{{Value: float64(s.ReplicaDesyncs())}}
		})
	return m
}

// publishDone é chamado pelo ackLoop depois de escrever o PUBACK.
func (m *brokerMetrics) publishDone(msg *protocol.Publish, res PublishResult, received time.Time) {
	m.ackLatency.ObserveDuration(time.Since(received))
	switch {
	case res.Stale:
		m.duplicated.Inc(msg.SensorID)
		return
	case res.Status == protocol.StatusNoSubscribers:
		m.noSubscribers.Inc(msg.SensorID)
	}
	m.published.Inc(msg.SensorID, msg.Topic)
}

func (m *brokerMetrics) deliverySent(latency time.Duration) {
	m.deliveryLatency.ObserveDuration(latency)
}

func (m *brokerMetrics) deliveryAcked() {
	m.delivered.Inc()
}

// sample monta uma amostra a partir de pares nome, valor.
func sample(value float64, pairs ...string) metrics.Sample {
	labels := make([]metrics.Label, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		labels = append(labels, metrics.Label{Name: pairs[i], Value: pairs[i+1]})
	}
	return metrics.Sample{Labels: labels, Value: value}
}
