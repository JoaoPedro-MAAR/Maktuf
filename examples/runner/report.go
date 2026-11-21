package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// writeSummary grava resumo.csv: uma linha por meta.
func writeSummary(path, scenario string, results []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"cenario", "meta", "tipo", "medido", "alvo", "atingida", "detalhe"})
	for _, r := range results {
		_ = w.Write([]string{scenario, r.goal.Name, r.goal.Type, r.measured, r.target, yesNo(r.ok), r.detail})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func yesNo(ok bool) string {
	if ok {
		return "sim"
	}
	return "nao"
}

// report reúne o que entra no relatorio.md.
type report struct {
	s        *Scenario
	started  time.Time
	elapsed  time.Duration
	faults   []appliedFault
	results  []result
	sensors  map[string]sensorSummary
	col      *collector
	procs    []string
	problems []string
}

func (r *report) write(path string) error {
	var b strings.Builder
	s := r.s
	fmt.Fprintf(&b, "# Cenário %s\n\n%s\n\n", s.Name, s.Description)
	fmt.Fprintf(&b, "- Início: %s\n- Duração da carga: %s (total com drenagem: %s)\n- Semente: %d\n",
		r.started.Format(time.RFC3339), s.Duration, r.elapsed.Round(time.Second), s.Seed)
	fmt.Fprintf(&b, "- Cluster: replicado=%v, juiz=%v\n\n", s.Cluster.Replicated, s.Cluster.Judge)

	if len(r.faults) > 0 {
		b.WriteString("## Falhas aplicadas\n\n| Previsto | Real | Ação | Alvo | Processo | Erro |\n|---|---|---|---|---|---|\n")
		for _, f := range r.faults {
			errText := ""
			if f.err != nil {
				errText = f.err.Error()
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n",
				f.At, f.when.Round(time.Millisecond), f.Action, f.Target, f.proc, errText)
		}
		b.WriteString("\n")
	}

	passed := 0
	b.WriteString("## Metas\n\n| Meta | Medido | Alvo | Atingida | Detalhe |\n|---|---|---|---|---|\n")
	for _, res := range r.results {
		mark := "❌"
		if res.ok {
			mark = "✅"
			passed++
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", res.goal.Name, res.measured, res.target, mark, res.detail)
	}
	fmt.Fprintf(&b, "\n**%d de %d metas atingidas.**\n\n", passed, len(r.results))

	b.WriteString("## Números gerais\n\n")
	groups := make([]string, 0, len(r.sensors))
	for g := range r.sensors {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	b.WriteString("| Sensores | Medidas | Confirmadas | Pendentes | Sem inscritos | Overflow | Falhas de conexão |\n|---|---|---|---|---|---|---|\n")
	for _, g := range groups {
		ss := r.sensors[g]
		fmt.Fprintf(&b, "| %s | %.0f | %.0f | %.0f | %.0f | %.0f | %.0f |\n",
			g, ss.measured, ss.confirmed, ss.pending, ss.noSubscribers, ss.overflow, ss.connectFailures)
	}
	b.WriteString("\n| Consumidor | Recebidas | Vazão média | Repetidas | Antigas | Lacunas sem marca | Lacunas por retenção |\n|---|---|---|---|---|---|---|\n")
	for _, c := range s.Consumers {
		p := "consumidor:" + c.ClientID
		received := r.col.sumLast(p, receivedMetric, nil)
		fmt.Fprintf(&b, "| %s | %.0f | %.2f/s | %.0f | %.0f | %.0f | %.0f |\n", c.ClientID, received,
			received/s.Duration.D().Seconds(),
			r.col.sumLast(p, "messages_duplicated_total", nil),
			r.col.sumLast(p, "messages_old_total", nil),
			r.col.sumLast(p, "missing_readings_total", map[string]string{"cause": "unmarked"}),
			r.col.sumLast(p, "missing_readings_total", map[string]string{"cause": "retention"}))
	}
	b.WriteString("\n| Broker | Maior fila de assinante | Descartes por retenção | Esperas de backpressure |\n|---|---|---|---|\n")
	for _, p := range r.procs {
		if p != "b1" && p != "b2" {
			continue
		}
		fmt.Fprintf(&b, "| %s | %.0f | %.0f | %.0f |\n", p,
			r.col.maxSum(p, "broker_queue_depth", map[string]string{"queue": "subscriber"}),
			r.col.sumLast(p, "retention_drops_total", nil),
			r.col.sumLast(p, "backpressure_waits_total", nil))
	}
	if s.Cluster.Judge {
		fmt.Fprintf(&b, "\nFailovers concluídos pelo Juiz: %.0f.", r.col.sumLast("juiz", "failover_duration_seconds_count", nil))
	}
	if s.Cluster.Replicated {
		fmt.Fprintf(&b, " CONNECTs recusados pelo vip: %.0f.\n", r.col.sumLast("vip", "vip_rejected_total", nil))
	}
	if len(r.problems) > 0 {
		b.WriteString("\n## Problemas na execução\n\n")
		for _, p := range r.problems {
			fmt.Fprintf(&b, "- %s\n", p)
		}
	}
	b.WriteString("\nSéries completas em `metricas.csv`; logs de cada processo nesta pasta.\n")
	return os.WriteFile(path, []byte(b.String()), 0o640)
}

// maxSum é o maior valor, ao longo do tempo, da soma das séries de um
// processo que casam com os rótulos.
func (c *collector) maxSum(proc, name string, labels map[string]string) float64 {
	c.mu.Lock()
	times := append([]time.Duration(nil), c.times...)
	c.mu.Unlock()
	var best float64
	for _, t := range times {
		if v := c.sumAt(proc, name, labels, t); v > best {
			best = v
		}
	}
	return best
}
