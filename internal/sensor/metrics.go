package sensor

import (
	"sync/atomic"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

// registerMetrics expõe os contadores e os buffers do simulador. Tudo é lido
// na hora do scrape, a partir do estado que o simulador já mantém.
func registerMetrics(reg *metrics.Registry, s *Simulator) {
	counter := func(name, help string, v *atomic.Uint64) {
		reg.NewCounterFunc(name, help, nil, func() []metrics.Sample {
			return []metrics.Sample{{Value: float64(v.Load())}}
		})
	}
	counter("sensor_readings_total", "Leituras geradas pelos sensores simulados.", &s.stats.measured)
	counter("messages_dropped_no_subscribers_total",
		"Leituras respondidas com NO_SUBSCRIBERS e removidas do buffer (RF-07).", &s.stats.noSubscribers)
	counter("sensor_publish_timeouts_total",
		"Medicoes que ficaram no buffer sem PUBACK dentro do prazo.", &s.stats.publishTimeouts)
	counter("sensor_connects_total", "Conexoes estabelecidas com o broker.", &s.stats.connects)
	counter("sensor_connect_failures_total", "Tentativas de conexao que falharam.", &s.stats.connectFailures)
	counter("sensor_simulated_failures_total", "Desconexoes simuladas.", &s.stats.failures)
	counter("sensor_sleeps_total", "Ciclos de sono (SLEEP).", &s.stats.sleeps)
	counter("sensor_late_wakes_total", "Sonos que passaram do prazo anunciado.", &s.stats.lateWakes)
	counter("sensor_burst_readings_total", "Leituras geradas pela rajada (RF-35, RNF-04).", &s.stats.burstReadings)

	reg.NewCounterFunc("sensor_readings_confirmed_total",
		"Leituras removidas do buffer por resposta definitiva do broker (inclui execucoes anteriores).",
		nil, func() []metrics.Sample {
			var total uint64
			for _, b := range s.bufferStats() {
				total += b.confirmed
			}
			return []metrics.Sample{{Value: float64(total)}}
		})
	reg.NewCounterFunc("sensor_buffer_overflow_total",
		"Leituras descartadas por overflow do buffer do sensor (RF-26, RF-31).",
		[]string{"sensor_id"}, func() []metrics.Sample {
			stats := s.bufferStats()
			out := make([]metrics.Sample, len(stats))
			for i, b := range stats {
				out[i] = metrics.Sample{Labels: sensorLabel(b.id), Value: float64(b.dropped)}
			}
			return out
		})
	reg.NewGaugeFunc("sensor_buffer_pending", "Leituras aguardando resposta definitiva no buffer.",
		[]string{"sensor_id"}, func() []metrics.Sample {
			stats := s.bufferStats()
			out := make([]metrics.Sample, len(stats))
			for i, b := range stats {
				out[i] = metrics.Sample{Labels: sensorLabel(b.id), Value: float64(b.pending)}
			}
			return out
		})
}

func sensorLabel(id string) []metrics.Label {
	return []metrics.Label{{Name: "sensor_id", Value: id}}
}
