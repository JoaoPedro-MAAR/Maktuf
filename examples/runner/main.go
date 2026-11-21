// Programa runner: executa um cenário de carga e falhas (task 017).
//
//	go run ./examples/runner -cenario examples/cenarios/queda_primario.json
//
// Compila os executáveis de cmd/, sobe Juiz, brokers, vip, consumidores e
// sensores como processos, injeta as falhas do cenário, coleta o /metrics de
// todos ao longo do tempo e avalia as metas. Os resultados ficam em
// <out>/<cenario>-<data>/ (metricas.csv, resumo.csv, relatorio.md e logs).
// Sai com 0 se todas as metas foram atingidas, 1 se alguma não foi e 2 em
// erro de execução.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	scenario := flag.String("cenario", "", "arquivo JSON do cenario (obrigatorio)")
	outRoot := flag.String("out", filepath.Join("data", "cenarios"), "diretorio dos resultados")
	flag.Parse()
	if *scenario == "" {
		fmt.Fprintln(os.Stderr, "uso: runner -cenario <arquivo.json> [-out dir]")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	outDir, results, err := run(ctx, *scenario, *outRoot, "", logger)
	if err != nil {
		logger.Error("cenario falhou", "error", err, "out", outDir)
		os.Exit(2)
	}
	failed := 0
	for _, r := range results {
		if !r.ok {
			failed++
		}
		fmt.Printf("%-4s %-40s medido: %-35s alvo: %s\n", map[bool]string{true: "OK", false: "FALHA"}[r.ok], r.goal.Name, r.measured, r.target)
	}
	fmt.Printf("\nresultados em %s\n", outDir)
	if failed > 0 {
		os.Exit(1)
	}
}

// run executa o cenário. binDir vazio compila os executáveis em <out>/bin.
func run(ctx context.Context, scenarioPath, outRoot, binDir string, log *slog.Logger) (string, []result, error) {
	s, err := Load(scenarioPath)
	if err != nil {
		return "", nil, err
	}
	outDir, err := filepath.Abs(filepath.Join(outRoot, s.Name+"-"+time.Now().Format("20060102-150405.000")))
	if err != nil {
		return "", nil, err
	}
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return outDir, nil, err
	}
	if binDir == "" {
		binDir = filepath.Join(outDir, "bin")
		log.Info("compilando executaveis", "dir", binDir)
		if err := build(binDir); err != nil {
			return outDir, nil, err
		}
	}

	cl, err := newCluster(s, outDir, binDir, log)
	if err != nil {
		return outDir, nil, err
	}
	defer cl.stopAll()

	log.Info("subindo o cluster", "cenario", s.Name)
	if err := cl.startInfra(ctx); err != nil {
		return outDir, nil, err
	}

	start := time.Now()
	col, err := newCollector(filepath.Join(outDir, "metricas.csv"), start)
	if err != nil {
		return outDir, nil, err
	}
	colCtx, stopCol := context.WithCancel(context.Background())
	colDone := make(chan struct{})
	go func() {
		defer close(colDone)
		col.run(colCtx, cl, s.ScrapeInterval.D())
	}()

	log.Info("carga iniciada", "cenario", s.Name, "duracao", s.Duration)
	faults, problems := runTimeline(ctx, s, cl, start, log)

	log.Info("encerrando sensores e drenando", "drain", s.Drain)
	cl.stopSensors()
	waitConsumers(ctx, s, cl, col)
	col.scrapeAll(cl)
	stopCol()
	<-colDone
	cl.stopAll()
	if err := col.close(); err != nil {
		return outDir, nil, err
	}

	sensors := map[string]sensorSummary{}
	for _, g := range s.Sensors {
		summary, err := readSensorSummary(cl.procs["sensores:"+g.Group].logPath)
		if err != nil {
			problems = append(problems, fmt.Sprintf("resumo dos sensores %s: %v", g.Group, err))
		}
		sensors[g.Group] = summary
	}

	ev := &evaluator{s: s, col: col, faults: faults, sensors: sensors, procs: cl.order}
	results := make([]result, 0, len(s.Goals))
	for _, g := range s.Goals {
		results = append(results, ev.evaluate(g))
	}

	if err := writeSummary(filepath.Join(outDir, "resumo.csv"), s.Name, results); err != nil {
		return outDir, results, err
	}
	rep := &report{s: s, started: start, elapsed: time.Since(start), faults: faults, results: results,
		sensors: sensors, col: col, procs: cl.order, problems: problems}
	if err := rep.write(filepath.Join(outDir, "relatorio.md")); err != nil {
		return outDir, results, err
	}
	if ctx.Err() != nil {
		return outDir, results, errors.New("cenario interrompido")
	}
	return outDir, results, nil
}

type event struct {
	at     time.Duration
	sensor string
	fault  int
}

// runTimeline inicia os sensores e aplica as falhas nos instantes previstos,
// até o fim da duração.
func runTimeline(ctx context.Context, s *Scenario, cl *cluster, start time.Time, log *slog.Logger) ([]appliedFault, []string) {
	var events []event
	for _, g := range s.Sensors {
		events = append(events, event{at: g.StartAt.D(), sensor: g.Group, fault: -1})
	}
	for i, f := range s.Faults {
		events = append(events, event{at: f.At.D(), fault: i})
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].at < events[j].at })

	faults := make([]appliedFault, len(s.Faults))
	for i, f := range s.Faults {
		faults[i] = appliedFault{Fault: f, err: errors.New("nao aplicada")}
	}
	var problems []string
	for _, ev := range events {
		if !sleepUntil(ctx, start.Add(ev.at)) {
			return faults, append(problems, "cenario interrompido")
		}
		if ev.fault < 0 {
			p := cl.procs["sensores:"+ev.sensor]
			if err := p.start(); err != nil {
				problems = append(problems, err.Error())
			}
			log.Info("sensores iniciados", "grupo", ev.sensor, "t", time.Since(start).Round(time.Millisecond))
			continue
		}
		f := s.Faults[ev.fault]
		at, proc, err := cl.apply(f)
		faults[ev.fault] = appliedFault{Fault: f, when: at.Sub(start), proc: proc, err: err}
		if err != nil {
			problems = append(problems, fmt.Sprintf("falha %d (%s %s): %v", ev.fault, f.Action, f.Target, err))
		}
		log.Info("falha aplicada", "acao", f.Action, "alvo", f.Target, "processo", proc,
			"t", at.Sub(start).Round(time.Millisecond), "error", err)
	}
	sleepUntil(ctx, start.Add(s.Duration.D()))
	for _, name := range cl.order {
		if p := cl.procs[name]; !p.running() && !strings.HasPrefix(name, "sensores:") && !wasTarget(faults, name) {
			problems = append(problems, name+" terminou sozinho durante o cenario (ver log)")
		}
	}
	return faults, problems
}

func wasTarget(faults []appliedFault, proc string) bool {
	for _, f := range faults {
		if f.proc == proc {
			return true
		}
	}
	return false
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// waitConsumers espera os consumidores pararem de receber (duas coletas sem
// mudança), limitado pelo tempo de drenagem.
func waitConsumers(ctx context.Context, s *Scenario, cl *cluster, col *collector) {
	deadline := time.Now().Add(s.Drain.D())
	last, stable := -1.0, 0
	for time.Now().Before(deadline) && stable < 2 {
		col.scrapeAll(cl)
		var total float64
		for _, c := range s.Consumers {
			total += col.sumLast("consumidor:"+c.ClientID, receivedMetric, nil)
		}
		if total == last {
			stable++
		} else {
			stable, last = 0, total
		}
		if !sleepUntil(ctx, time.Now().Add(s.ScrapeInterval.D())) {
			return
		}
	}
}
