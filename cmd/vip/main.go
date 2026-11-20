// Programa vip: endereço virtual dos brokers (task 013). Os clientes usam só
// este endereço; o vip leva cada conexão ao broker que aceita o CONNECT.
//
//	go run ./cmd/vip -addr :7070 -backends 127.0.0.1:7000,127.0.0.1:7001
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("vip encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var cfg vip.Config
	flag.StringVar(&cfg.Addr, "addr", vip.DefaultAddr, "endereco que os clientes usam")
	backends := flag.String("backends", "", "enderecos dos brokers separados por virgula, na ordem de tentativa (obrigatorio)")
	flag.DurationVar(&cfg.DialTimeout, "dial-timeout", vip.DefaultDialTimeout, "limite da conexao com cada broker")
	flag.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", vip.DefaultHandshakeTimeout, "limite do CONNECT do cliente e do CONNACK de cada broker")
	metricsAddr := flag.String("metrics-addr", "", "endereco HTTP do /metrics (vazio = desligado)")
	flag.Parse()

	for _, b := range strings.Split(*backends, ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Backends = append(cfg.Backends, b)
		}
	}
	if len(cfg.Backends) == 0 {
		return errors.New("-backends e obrigatorio")
	}
	cfg.Logger = logger
	p, err := vip.New(cfg)
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
			if err := metrics.Serve(ctx, ln, p.Metrics(), logger); err != nil {
				logger.Error("endpoint /metrics encerrou", "error", err)
			}
		}()
	}

	return p.ListenAndServe(ctx)
}
