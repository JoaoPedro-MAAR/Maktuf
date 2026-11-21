package consumer

import (
	"strings"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

// Valores do rótulo cause das métricas de lacuna.
const (
	causeRetention = "retention"
	causeUnmarked  = "unmarked"
)

// consumerMetrics traduz cada Observation do Tracker em contadores. O
// Tracker continua sendo a regra; aqui só se conta.
type consumerMetrics struct {
	received    *metrics.Counter
	duplicated  *metrics.Counter
	old         *metrics.Counter
	gaps        *metrics.Counter
	missing     *metrics.Counter
	transitions *metrics.Counter
	endToEnd    *metrics.Histogram
}

func newConsumerMetrics(reg *metrics.Registry) *consumerMetrics {
	return &consumerMetrics{
		received: reg.NewCounter("messages_received_total",
			"Leituras novas aceitas pelo consumidor, por grupo de sensores.", "group"),
		duplicated: reg.NewCounter("messages_duplicated_total",
			"Leituras repetidas descartadas pela deduplicacao sensor_id + seq (RF-14, CE-15)."),
		old: reg.NewCounter("messages_old_total",
			"Leituras com seq ja processado fora da janela de deduplicacao (CE-16)."),
		gaps: reg.NewCounter("sequence_gaps_total",
			"Saltos de seq detectados (RF-22); cause=retention veio com GAP_BEFORE.", "cause"),
		missing: reg.NewCounter("missing_readings_total",
			"Total de seq faltantes nas lacunas detectadas.", "cause"),
		transitions: reg.NewCounter("sensor_status_transitions_total",
			"Transicoes recebidas do topico sensores/{id}/status (RF-21).", "state"),
		endToEnd: reg.NewHistogram("end_to_end_latency_seconds",
			"Tempo entre a medicao no sensor e o recebimento (inclui o tempo no buffer do sensor), por grupo de sensores.", nil, "group"),
	}
}

// sensorGroup é o grupo do sensor nas métricas: o sensor_id sem o último
// trecho depois de "-" (sensor-007 → sensor, agressivo-001 → agressivo). Com
// isso o p95 de cada grupo do simulador pode ser comparado (RNF-05) sem um
// rótulo por sensor.
func sensorGroup(id string) string {
	if i := strings.LastIndexByte(id, '-'); i > 0 {
		return id[:i]
	}
	return id
}

func (m *consumerMetrics) observe(obs Observation, msg client.Message) {
	switch obs.Outcome {
	case OutcomeStatus:
		m.transitions.Inc(msg.Status.String())
	case OutcomeDuplicate:
		m.duplicated.Inc()
	case OutcomeOld:
		m.old.Inc()
	case OutcomeAccepted:
		group := sensorGroup(msg.SensorID)
		m.received.Inc(group)
		m.endToEnd.ObserveDuration(obs.Latency, group)
		cause := causeUnmarked
		if obs.Marked {
			cause = causeRetention
		}
		if obs.Missing > 0 {
			m.gaps.Inc(cause)
			m.missing.Add(obs.Missing, cause)
		} else if obs.Marked {
			// Mesmo critério do Tracker: lacuna avisada sem referência.
			m.gaps.Inc(cause)
		}
	}
}
