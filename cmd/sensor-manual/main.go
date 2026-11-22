// Programa sensor-manual: um único sensor controlado pelo teclado, para
// demonstrar o sistema ao vivo (publicar, dormir, cair, reconectar, rajada).
//
//	go run ./cmd/sensor-manual -addr 127.0.0.1:7070 -id sensor-demo
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "sensor-manual:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg sensor.ManualConfig
	flag.StringVar(&cfg.Addr, "addr", vip.DefaultAddr, "endereco do vip (ambiente completo); para um broker sozinho, o endereco dele, ex.: 127.0.0.1:7000")
	flag.StringVar(&cfg.SensorID, "id", "sensor-manual", "id do sensor (topico sensores/<id>/temperatura)")
	dataDir := flag.String("data-dir", filepath.Join("data", "sensor-manual"), "diretorio do buffer persistente")
	flag.IntVar(&cfg.BufferSize, "buffer-size", sensor.DefaultBufferSize, "maximo de leituras pendentes no buffer")
	flag.DurationVar(&cfg.PublishTimeout, "publish-timeout", sensor.DefaultPublishTimeout, "espera maxima pelo PUBACK")
	flag.DurationVar(&cfg.Reconnect.MaxDelay, "reconnect-max", client.DefaultReconnectMaxDelay, "intervalo maximo de reconexao")
	debug := flag.Bool("debug", false, "mostra o log da biblioteca no stderr")
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o750); err != nil {
		return err
	}
	cfg.BufferPath = filepath.Join(*dataDir, cfg.SensorID+".buf")
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if *debug {
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	m, err := sensor.NewManual(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return m.Run(ctx, os.Stdin, os.Stdout)
}
