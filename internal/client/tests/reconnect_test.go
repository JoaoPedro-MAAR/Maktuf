package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

func waitConnected(t *testing.T, c *client.Client) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !c.Connected() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando reconexao; Err=%v", c.Err())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitPending(t *testing.T, b storage.Buffer, want int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for b.Stats().Pending != want {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %d leituras pendentes; buffer=%+v", want, b.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReconnectRestoresSubscriptions(t *testing.T) {
	addr := freeAddr(t)
	b := startBrokerAt(t, addr, broker.Config{})

	consumer := connect(t, addr, client.RoleConsumer, "consumidor-1")
	if err := consumer.Subscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sensor := connect(t, addr, client.RoleSensor, "sensor-a")

	if got := publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 1}); got != client.StatusPersisted {
		t.Fatalf("status inicial = %s", got)
	}
	if msg := receive(t, consumer); msg.Seq != 1 {
		t.Fatalf("seq = %d, esperado 1", msg.Seq)
	}

	b.stop(t)
	startBrokerAt(t, addr, broker.Config{})

	waitConnected(t, consumer)
	waitConnected(t, sensor)

	if got := publish(t, sensor, sensorTopic, client.Reading{SensorID: "sensor-a", Seq: 2}); got != client.StatusPersisted {
		t.Fatalf("status apos reconexao = %s", got)
	}
	if msg := receive(t, consumer); msg.Seq != 2 {
		t.Fatalf("seq = %d, esperado 2 sem novo SUBSCRIBE", msg.Seq)
	}
}

func TestReconnectResendsPendingSensorBuffer(t *testing.T) {
	addr := freeAddr(t)
	dataDir := t.TempDir()
	b := startBrokerAt(t, addr, broker.Config{DataDir: dataDir})

	consumer := connect(t, addr, client.RoleConsumer, "consumidor-1")
	if err := consumer.Subscribe(testContext(t), sensorTopic); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	buffer, err := storage.OpenSensorBuffer(filepath.Join(t.TempDir(), "sensor.wal"), storage.BufferOptions{
		MaxReadings: 100,
		SensorID:    "sensor-a",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("OpenSensorBuffer: %v", err)
	}
	t.Cleanup(func() { _ = buffer.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	sensor, err := client.ConnectWithOptions(ctx, addr, client.RoleSensor, "sensor-a", client.Options{
		Reconnect: client.ReconnectOptions{
			BaseDelay:   10 * time.Millisecond,
			MaxDelay:    50 * time.Millisecond,
			DialTimeout: time.Second,
		},
		Sensor: &client.SensorOptions{Buffer: buffer, Topic: sensorTopic},
	})
	if err != nil {
		t.Fatalf("ConnectWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = sensor.Close() })

	b.stop(t)

	type result struct {
		reading storage.Reading
		status  client.Status
		err     error
	}
	results := make(chan result, 1)
	go func() {
		reading, status, err := sensor.PublishBuffered(ctx, time.Now(), 42.5)
		results <- result{reading: reading, status: status, err: err}
	}()

	waitPending(t, buffer, 1)
	startBrokerAt(t, addr, broker.Config{DataDir: dataDir})

	select {
	case got := <-results:
		if got.err != nil {
			t.Fatalf("PublishBuffered: %v", got.err)
		}
		if got.status != client.StatusPersisted {
			t.Fatalf("status = %s, esperado PERSISTED", got.status)
		}
		if got.reading.Seq != 1 {
			t.Fatalf("seq = %d, esperado 1", got.reading.Seq)
		}
	case <-time.After(timeout):
		t.Fatalf("timeout aguardando reenvio da leitura")
	}

	msg := receive(t, consumer)
	if msg.SensorID != "sensor-a" || msg.Seq != 1 || msg.Temperature != 42.5 {
		t.Fatalf("mensagem inesperada: %+v", msg)
	}
	waitPending(t, buffer, 0)
}

func TestReconnectStopsAfterMaxAttempts(t *testing.T) {
	addr := freeAddr(t)
	b := startBrokerAt(t, addr, broker.Config{})

	c, err := client.ConnectWithOptions(testContext(t), addr, client.RoleSensor, "sensor-a", client.Options{
		Reconnect: client.ReconnectOptions{
			BaseDelay:   10 * time.Millisecond,
			MaxDelay:    20 * time.Millisecond,
			DialTimeout: 50 * time.Millisecond,
			MaxAttempts: 2,
		},
	})
	if err != nil {
		t.Fatalf("ConnectWithOptions: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	b.stop(t)

	select {
	case <-c.Done():
	case <-time.After(timeout):
		t.Fatalf("Done nao foi fechado apos esgotar tentativas")
	}
	if !errors.Is(c.Err(), client.ErrReconnectExhausted) {
		t.Fatalf("Err = %v, esperado ErrReconnectExhausted", c.Err())
	}
}
