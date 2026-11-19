package main

import (
	"context"
	"errors"
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
	dataDir := flag.String("data-dir", "data/broker", "diretorio onde o broker grava seu estado em disco")
	sensorQueue := flag.Int("sensor-queue", broker.DefaultSensorQueueLimit, "PUBLISH sem PUBACK permitidos por sensor")
	subscriberQueue := flag.Int("subscriber-queue", broker.DefaultSubscriberQueueLimit, "retencao de leituras por assinante")
	flag.Parse()

	if *dataDir == "" {
		return errors.New("-data-dir nao pode ser vazio")
	}
	server := broker.NewServer(broker.Config{
		Addr:                 *addr,
		DataDir:              *dataDir,
		SensorQueueLimit:     *sensorQueue,
		SubscriberQueueLimit: *subscriberQueue,
		Logger:               logger,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return server.ListenAndServe(ctx)
}
