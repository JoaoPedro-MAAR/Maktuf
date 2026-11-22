// Programa painel: aplicação de exemplo com interface web. Sensores e um
// consumidor são comandados pela página; brokers, Juiz e vip são observados.
//
//	go run ./cmd/painel -sensores sala-01,lab-02
//	# abrir http://127.0.0.1:8080
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
	"strings"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/painel"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	if err := run(logger); err != nil {
		logger.Error("painel encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var cfg painel.Config
	flag.StringVar(&cfg.Addr, "addr", painel.DefaultAddr, "endereco HTTP da pagina")
	flag.StringVar(&cfg.ClientAddr, "cliente-addr", vip.DefaultAddr, "onde sensores e consumidor conectam (o vip; ou um broker sozinho)")
	flag.StringVar(&cfg.DataDir, "data-dir", filepath.Join("data", "painel"), "diretorio dos buffers dos sensores")
	flag.StringVar(&cfg.ConsumerID, "consumidor-id", painel.DefaultConsumerID, "id do consumidor do painel")
	sensors := flag.String("sensores", "", "sensores criados ao iniciar, separados por virgula")
	filters := flag.String("filtros", strings.Join(painel.DefaultFilters, ","), "inscricoes iniciais do consumidor, separadas por virgula")
	flag.StringVar(&cfg.JudgeMetrics, "juiz-metrics", "127.0.0.1:9103", "endereco do /metrics do juiz (vazio = nao observar)")
	brokers := flag.String("broker-metrics", "b1=127.0.0.1:9100,b2=127.0.0.1:9101", "brokers observados: id=endereco do /metrics, separados por virgula")
	flag.Parse()

	cfg.Sensors = split(*sensors)
	cfg.Filters = split(*filters)
	for _, b := range split(*brokers) {
		id, addr, ok := strings.Cut(b, "=")
		if !ok || id == "" || addr == "" {
			return errors.New("-broker-metrics: use id=endereco, por exemplo b1=127.0.0.1:9100")
		}
		cfg.Brokers = append(cfg.Brokers, painel.BrokerEndpoint{ID: id, Metrics: addr})
	}
	cfg.Logger = logger

	p, err := painel.New(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := p.Start(ctx); err != nil {
		return fmt.Errorf("iniciar o painel: %w", err)
	}
	defer func() { _ = p.Close() }()
	fmt.Printf("painel em http://%s (clientes via %s)\n", cfg.Addr, cfg.ClientAddr)
	return p.ListenAndServe(ctx)
}

func split(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
