// Programa consumidor: serviço consumidor de exemplo (RF-60).
//
//	go run ./cmd/consumidor -addr 127.0.0.1:7000
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("consumidor encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg consumer.Config
	flag.StringVar(&cfg.Addr, "addr", broker.DefaultAddr, "endereco TCP do broker")
	flag.StringVar(&cfg.ClientID, "client-id", consumer.DefaultClientID, "identificador do consumidor (mantem o cursor no broker)")
	filters := flag.String("filters", strings.Join(consumer.DefaultFilters, ","), "filtros de topico separados por virgula")
	flag.IntVar(&cfg.DedupWindow, "dedup-window", client.DefaultDedupLimit, "chaves sensor_id+seq lembradas pela deduplicacao")
	flag.IntVar(&cfg.LatencySamples, "latency-samples", consumer.DefaultLatencySamples, "tamanho da amostra de latencias")
	flag.DurationVar(&cfg.RetryInterval, "retry", consumer.DefaultRetryInterval, "espera entre tentativas da conexao inicial")
	flag.DurationVar(&cfg.Reconnect.BaseDelay, "reconnect-base", client.DefaultReconnectBaseDelay, "intervalo inicial de reconexao")
	flag.DurationVar(&cfg.Reconnect.MaxDelay, "reconnect-max", client.DefaultReconnectMaxDelay, "intervalo maximo de reconexao")
	flag.Float64Var(&cfg.Reconnect.Jitter, "reconnect-jitter", client.DefaultReconnectJitter, "variacao aleatoria do intervalo de reconexao (0 a 1)")
	flag.DurationVar(&cfg.Reconnect.DialTimeout, "dial-timeout", client.DefaultReconnectDial, "limite de cada tentativa de conexao")
	duration := flag.Duration("duration", 0, "tempo de execucao (0 = ate SIGINT/SIGTERM)")
	report := flag.Duration("report", 10*time.Second, "intervalo do resumo periodico (0 = so no fim)")
	debug := flag.Bool("debug", false, "loga cada leitura")
	flag.Parse()

	for _, f := range strings.Split(*filters, ",") {
		if f = strings.TrimSpace(f); f != "" {
			cfg.Filters = append(cfg.Filters, f)
		}
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	cfg.Logger = logger

	cons, err := consumer.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if *report > 0 {
		go func() {
			t := time.NewTicker(*report)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					logStats(logger, "resumo parcial", cons.Stats())
				}
			}
		}()
	}

	err = cons.Run(ctx)
	logStats(logger, "resumo final", cons.Stats())
	return err
}

func logStats(logger *slog.Logger, msg string, st consumer.Stats) {
	logger.Info(msg,
		"sensores", st.Sensors,
		"recebidas", st.Received,
		"repetidas", st.Duplicates,
		"antigas", st.Old,
		"lacunas_sem_marca", st.GapsUnmarked,
		"faltando_sem_marca", st.MissingUnmarked,
		"lacunas_retencao", st.GapsMarked,
		"faltando_retencao", st.MissingMarked,
		"status_online", st.StatusOnline,
		"status_dormindo", st.StatusDormindo,
		"status_offline", st.StatusOffline,
		"latencia_p50", st.LatencyP50,
		"latencia_p95", st.LatencyP95,
		"latencia_p99", st.LatencyP99,
		"latencia_max", st.LatencyMax,
	)
}
