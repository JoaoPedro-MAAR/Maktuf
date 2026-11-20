package juiz

import (
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

type judgeMetrics struct {
	failover   *metrics.Histogram
	promotions *metrics.Counter
}

func newJudgeMetrics(reg *metrics.Registry, j *Judge) *judgeMetrics {
	m := &judgeMetrics{
		failover: reg.NewHistogram("failover_duration_seconds",
			"Tempo entre detectar o primario sem heartbeat e a reserva promovida anunciar estado primario.", nil),
		promotions: reg.NewCounter("judge_promotions_total",
			"PROMOTE enviados para promover uma reserva.", "broker_id"),
	}
	reg.NewGaugeFunc("judge_broker_state",
		"Ultimo estado anunciado por broker (0 quando sem heartbeat dentro do prazo de falha).",
		[]string{"broker_id"}, func() []metrics.Sample {
			views, now := j.snapshot()
			out := make([]metrics.Sample, 0, len(views))
			for id, v := range views {
				state := 0.0
				if now.Sub(v.lastSeen) <= j.cfg.FailureTimeout {
					state = float64(v.state)
				}
				out = append(out, metrics.Sample{
					Labels: []metrics.Label{{Name: "broker_id", Value: id}},
					Value:  state,
				})
			}
			return out
		})
	return m
}
