// Programa sensor: simulador de sensores de temperatura (RF-59).
//
// Exemplo com o perfil de referência do RNF-03 (200 sensores, 10% contínuos):
//
//	go run ./cmd/sensor -count 200 -seed 42
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
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/vip"
)

func main() {
	if err := run(); err != nil {
		slog.Error("simulador encerrou com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg sensor.Config
	flag.StringVar(&cfg.Addr, "addr", vip.DefaultAddr, "endereco do vip (ambiente completo); para um broker sozinho, o endereco dele, ex.: 127.0.0.1:7000")
	flag.IntVar(&cfg.Count, "count", 10, "quantidade de sensores simulados")
	flag.StringVar(&cfg.Topic, "topico", "", "topico de publicacao; {id} vira o id do sensor (padrao sensores/{id}/temperatura); sem {id}, todos os sensores publicam no mesmo topico")
	flag.StringVar(&cfg.IDPrefix, "id-prefix", sensor.DefaultIDPrefix, "prefixo dos ids dos sensores (<prefixo>-001, ...)")
	flag.StringVar(&cfg.DataDir, "data-dir", "data/sensores", "diretorio dos buffers persistentes (um arquivo por sensor)")
	flag.IntVar(&cfg.BufferSize, "buffer-size", sensor.DefaultBufferSize, "maximo de leituras pendentes por sensor (padrao: 24 h de sensor continuo)")
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
	flag.DurationVar(&cfg.BurstAt, "burst-at", 0, "instante da rajada, contado do inicio")
	flag.IntVar(&cfg.BurstSize, "burst-size", 0, "leituras da rajada no total, divididas entre os sensores (0 = sem rajada)")
	flag.DurationVar(&cfg.DrainTimeout, "drain-timeout", sensor.DefaultDrainTimeout, "espera para enviar as pendencias ao encerrar (0 = nao drena)")
	flag.DurationVar(&cfg.Reconnect.BaseDelay, "reconnect-base", client.DefaultReconnectBaseDelay, "intervalo inicial de reconexao")
	flag.DurationVar(&cfg.Reconnect.MaxDelay, "reconnect-max", client.DefaultReconnectMaxDelay, "intervalo maximo de reconexao")
	flag.Float64Var(&cfg.Reconnect.Jitter, "reconnect-jitter", client.DefaultReconnectJitter, "variacao aleatoria do intervalo de reconexao (0 a 1)")
	flag.DurationVar(&cfg.Reconnect.DialTimeout, "dial-timeout", client.DefaultReconnectDial, "limite de cada tentativa de conexao")
	duration := flag.Duration("duration", 0, "tempo de execucao (0 = ate SIGINT/SIGTERM)")
	report := flag.Duration("report", 10*time.Second, "intervalo do resumo periodico (0 = so no fim)")
	debug := flag.Bool("debug", false, "loga cada leitura")
	metricsAddr := flag.String("metrics-addr", "", "endereco HTTP do /metrics (vazio = desligado)")
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

	if *metricsAddr != "" {
		ln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("endereco do -metrics-addr: %w", err)
		}
		go func() {
			if err := metrics.Serve(ctx, ln, sim.Metrics(), logger); err != nil {
				logger.Error("endpoint /metrics encerrou", "error", err)
			}
		}()
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
