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
	"syscall"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/replication"
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
	statusTolerance := flag.Duration("status-tolerance", broker.DefaultStatusTolerance, "tolerancia somada ao prazo do SLEEP")
	statusScan := flag.Duration("status-scan", broker.DefaultStatusScanInterval, "intervalo do monitor de prazos de sono")
	role := flag.String("role", string(broker.RolePrimary), "papel inicial do broker: primario ou reserva (com juiz, o papel muda no failover)")
	replicaAddr := flag.String("replica-addr", "", "endereco de replicacao do par, usado quando este broker e primario (vazio = sem reserva)")
	replicationAddr := flag.String("replication-addr", "", "endereco onde este broker escuta a replicacao, usado quando e reserva")
	replicaTimeout := flag.Duration("replica-timeout", replication.DefaultTimeout, "espera maxima pelo REPLICA_ACK da reserva")
	brokerID := flag.String("id", "", "identificador do broker para o juiz (obrigatorio com -judge-addr)")
	judgeAddr := flag.String("judge-addr", "", "endereco do juiz (vazio = sem juiz e sem failover)")
	heartbeat := flag.Duration("heartbeat-interval", broker.DefaultHeartbeatInterval, "periodo do HEARTBEAT ao juiz")
	fenceTimeout := flag.Duration("fence-timeout", broker.DefaultFenceTimeout,
		"espera pelo aval do juiz sem reserva antes de se isolar (use menor que o -failure-timeout do juiz)")
	metricsAddr := flag.String("metrics-addr", "", "endereco HTTP do /metrics (vazio = desligado)")
	flag.Parse()

	if *dataDir == "" {
		return errors.New("-data-dir nao pode ser vazio")
	}
	server := broker.NewServer(broker.Config{
		Addr:                 *addr,
		DataDir:              *dataDir,
		SensorQueueLimit:     *sensorQueue,
		SubscriberQueueLimit: *subscriberQueue,
		StatusTolerance:      *statusTolerance,
		StatusScanInterval:   *statusScan,
		Role:                 broker.Role(*role),
		ReplicaAddr:          *replicaAddr,
		ReplicationAddr:      *replicationAddr,
		ReplicaTimeout:       *replicaTimeout,
		BrokerID:             *brokerID,
		JudgeAddr:            *judgeAddr,
		HeartbeatInterval:    *heartbeat,
		FenceTimeout:         *fenceTimeout,
		Logger:               logger,
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *metricsAddr != "" {
		ln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			return fmt.Errorf("endereco do -metrics-addr: %w", err)
		}
		go func() {
			if err := metrics.Serve(ctx, ln, server.Metrics(), logger); err != nil {
				logger.Error("endpoint /metrics encerrou", "error", err)
			}
		}()
	}

	return server.ListenAndServe(ctx)
}
