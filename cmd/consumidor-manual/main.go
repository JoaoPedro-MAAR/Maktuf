// Programa consumidor-manual: um consumidor controlado pelo teclado, para
// demonstrar ao vivo inscrição, cancelamento e retenção.
//
//	go run ./cmd/consumidor-manual -id painel
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "consumidor-manual:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg consumer.ManualConfig
	flag.StringVar(&cfg.Addr, "addr", vip.DefaultAddr, "endereco do vip (ambiente completo); para um broker sozinho, o endereco dele, ex.: 127.0.0.1:7000")
	flag.StringVar(&cfg.ClientID, "id", consumer.DefaultManualClientID, "id do consumidor; as inscricoes ficam gravadas no broker por id")
	flag.DurationVar(&cfg.Reconnect.MaxDelay, "reconnect-max", client.DefaultReconnectMaxDelay, "intervalo maximo de reconexao")
	debug := flag.Bool("debug", false, "mostra o log da biblioteca no stderr")
	flag.Parse()

	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if *debug {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	m, err := consumer.NewManual(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return m.Run(ctx, os.Stdin, os.Stdout)
}
