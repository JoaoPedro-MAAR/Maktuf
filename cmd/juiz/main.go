// Programa juiz: coordenador de failover do par primário/reserva (task 012).
//
//	go run ./cmd/juiz -addr :7100 -failure-timeout 5s
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/juiz"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("juiz encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var cfg juiz.Config
	flag.StringVar(&cfg.Addr, "addr", juiz.DefaultAddr, "endereco TCP onde os brokers enviam HEARTBEAT")
	flag.DurationVar(&cfg.FailureTimeout, "failure-timeout", juiz.DefaultFailureTimeout,
		"tempo sem HEARTBEAT para considerar um broker fora do ar (use maior que o -fence-timeout dos brokers)")
	flag.DurationVar(&cfg.CheckInterval, "check-interval", juiz.DefaultCheckInterval, "periodo da avaliacao de failover")
	metricsAddr := flag.String("metrics-addr", "", "endereco HTTP do /metrics (vazio = desligado)")
	flag.Parse()

	cfg.Logger = logger
	j, err := juiz.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *metricsAddr != "" {
		ln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("endereco do -metrics-addr: %w", err)
		}
		go func() {
			if err := metrics.Serve(ctx, ln, j.Metrics(), logger); err != nil {
				logger.Error("endpoint /metrics encerrou", "error", err)
			}
		}()
	}

	return j.ListenAndServe(ctx)
}
