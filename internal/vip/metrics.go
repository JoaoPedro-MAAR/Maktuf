package vip

import (
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

type vipMetrics struct {
	routed   *metrics.Counter
	rejected *metrics.Counter
}

func newVipMetrics(reg *metrics.Registry, p *Proxy) *vipMetrics {
	reg.NewGaugeFunc("vip_connections", "Pares cliente-broker ativos no vip.", nil,
		func() []metrics.Sample {
			return []metrics.Sample{{Value: float64(p.pairs.Load())}}
		})
	return &vipMetrics{
		routed: reg.NewCounter("vip_routed_total",
			"Conexoes de clientes entregues a cada broker (a troca de broker marca um failover).", "backend"),
		rejected: reg.NewCounter("vip_rejected_total",
			"CONNECT recusado porque nenhum broker aceitou (janela de indisponibilidade)."),
	}
}
