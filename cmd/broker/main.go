package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("broker encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	addr := flag.String("addr", broker.DefaultAddr, "endereco TCP do broker")
	flag.Parse()

	server := broker.NewServer(broker.Config{Addr: *addr, Logger: logger})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return server.ListenAndServe(ctx)
}
