// Programa sensor: simulador de sensores de temperatura (RF-59).
//
// Exemplo com o perfil de referência do RNF-03 (200 sensores, 10% contínuos):
//
//	go run ./cmd/sensor -count 200 -seed 42
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
)

func main() {
	if err := run(); err != nil {
		slog.Error("simulador encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg sensor.Config
	flag.StringVar(&cfg.Addr, "addr", broker.DefaultAddr, "endereco TCP do broker")
	flag.IntVar(&cfg.Count, "count", 10, "quantidade de sensores simulados")
	flag.StringVar(&cfg.IDPrefix, "id-prefix", sensor.DefaultIDPrefix, "prefixo dos ids dos sensores (<prefixo>-001, ...)")
	flag.StringVar(&cfg.DataDir, "data-dir", "data/sensores", "diretorio dos buffers persistentes (um arquivo por sensor)")
	flag.IntVar(&cfg.BufferSize, "buffer-size", sensor.DefaultBufferSize, "maximo de leituras pendentes por sensor (provisorio, requisitos 11.1 item 5)")
	flag.Float64Var(&cfg.ContinuousRatio, "continuous-ratio", sensor.DefaultContinuousRatio, "fracao de sensores com perfil continuo")
	flag.DurationVar(&cfg.LowInterval, "low-interval", sensor.DefaultLowInterval, "intervalo de medicao do perfil baixo")
	flag.DurationVar(&cfg.ContinuousInterval, "continuous-interval", sensor.DefaultContinuousInterval, "intervalo de medicao do perfil continuo")
	flag.BoolVar(&cfg.Stagger, "stagger", true, "espalha a primeira medicao de cada sensor dentro do intervalo")
	flag.Int64Var(&cfg.Seed, "seed", 1, "semente dos sorteios (mesma semente, mesmo cenario)")
	flag.Float64Var(&cfg.FailureProb, "failure-prob", 0, "chance de desconexao por medicao")
	flag.DurationVar(&cfg.FailureDowntime, "failure-downtime", sensor.DefaultFailureDowntime, "tempo sem conectar apos uma desconexao simulada")
	flag.Float64Var(&cfg.SleepProb, "sleep-prob", 0, "chance de entrar em sono por medicao")
	flag.DurationVar(&cfg.SleepDuration, "sleep-duration", sensor.DefaultSleepDuration, "duracao anunciada no SLEEP")
	flag.Float64Var(&cfg.LateWakeProb, "late-wake-prob", 0, "chance de acordar depois do prazo do SLEEP")
	flag.DurationVar(&cfg.LateWakeExtra, "late-wake-extra", sensor.DefaultLateWakeExtra, "atraso alem do prazo ao acordar tarde")
	flag.DurationVar(&cfg.PublishTimeout, "publish-timeout", sensor.DefaultPublishTimeout, "espera maxima pelo PUBACK de cada medicao")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", sensor.DefaultDrainTimeout, "espera para enviar as pendencias ao encerrar (0 = nao drena)")
	flag.DurationVar(&cfg.Reconnect.BaseDelay, "reconnect-base", client.DefaultReconnectBaseDelay, "intervalo inicial de reconexao")
	flag.DurationVar(&cfg.Reconnect.MaxDelay, "reconnect-max", client.DefaultReconnectMaxDelay, "intervalo maximo de reconexao")
	flag.Float64Var(&cfg.Reconnect.Jitter, "reconnect-jitter", client.DefaultReconnectJitter, "variacao aleatoria do intervalo de reconexao (0 a 1)")
	flag.DurationVar(&cfg.Reconnect.DialTimeout, "dial-timeout", client.DefaultReconnectDial, "limite de cada tentativa de conexao")
	duration := flag.Duration("duration", 0, "tempo de execucao (0 = ate SIGINT/SIGTERM)")
	report := flag.Duration("report", 10*time.Second, "intervalo do resumo periodico (0 = so no fim)")
	debug := flag.Bool("debug", false, "loga cada leitura")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
	cfg.Logger = logger

	sim, err := sensor.New(cfg)
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
					logStats(logger, "resumo parcial", sim.Stats())
				}
			}
		}()
	}

	logger.Info("simulador iniciado", "sensores", cfg.Count, "seed", cfg.Seed, "data_dir", cfg.DataDir)
	err = sim.Run(ctx)
	logStats(logger, "resumo final", sim.Stats())
	return err
}

func logStats(logger *slog.Logger, msg string, st sensor.Stats) {
	logger.Info(msg,
		"sensores", st.Sensors,
		"medidas", st.Measured,
		"persistidas", st.Persisted,
		"sem_inscritos", st.NoSubscribers,
		"sem_puback", st.PublishTimeouts,
		"confirmadas_buffer", st.Confirmed,
		"pendentes", st.Pending,
		"descartes_overflow", st.Dropped,
		"conexoes", st.Connects,
		"falhas_conexao", st.ConnectFailures,
		"desconexoes_simuladas", st.Failures,
		"sonos", st.Sleeps,
		"acordou_atrasado", st.LateWakes,
	)
}
