package client_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

// Um consumidor se inscreve e lê as entregas pelo canal Messages. Depois de
// uma queda, a biblioteca reconecta e restaura a inscrição sozinha.
func ExampleConnect() {
	ctx := context.Background()
	c, err := client.Connect(ctx, "127.0.0.1:7070", client.RoleConsumer, "consumidor-1")
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = c.Close() }()

	if err := c.Subscribe(ctx, "sensores/+/temperatura"); err != nil {
		log.Println(err)
		return
	}
	if err := c.Subscribe(ctx, "sensores/+/status"); err != nil {
		log.Println(err)
		return
	}

	dedup := client.NewDedup(0)
	for m := range c.Messages() {
		if !dedup.AcceptReading(m) {
			continue // repetida: a entrega é pelo menos uma vez
		}
		if m.Status != client.SensorStateUnknown {
			fmt.Println(m.SensorID, "agora esta", m.Status)
			continue
		}
		if m.Gap {
			fmt.Println("leituras de", m.SensorID, "descartadas por retencao antes desta")
		}
		fmt.Println(m.SensorID, m.Seq, m.Temperature, m.Timestamp)
	}
}

// Um sensor com buffer durável: cada leitura vai primeiro para o disco e só
// sai dele com resposta definitiva do broker. O que ficar pendente é
// reenviado em ordem na reconexão, inclusive depois de reiniciar o processo.
func ExampleConnectWithOptions() {
	buffer, err := storage.OpenSensorBuffer("data/sensor-a.buf", storage.BufferOptions{
		MaxReadings: 10000,
		SensorID:    "sensor-a",
	})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = buffer.Close() }()

	ctx := context.Background()
	c, err := client.ConnectWithOptions(ctx, "127.0.0.1:7070", client.RoleSensor, "sensor-a", client.Options{
		Reconnect: client.ReconnectOptions{BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second},
		Sensor:    &client.SensorOptions{Buffer: buffer, Topic: "sensores/sensor-a/temperatura"},
	})
	if err != nil {
		log.Println(err)
		return
	}
	defer func() { _ = c.Close() }()

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reading, status, err := c.PublishBuffered(pubCtx, time.Now(), 23.5)
	if err != nil {
		// A leitura continua no buffer e será reenviada.
		log.Println("sem PUBACK por enquanto:", err)
		return
	}
	fmt.Println("seq", reading.Seq, "status", status)

	// Antes de desligar o rádio, o sensor avisa o broker (RF-27).
	if err := c.Sleep(ctx, 5*time.Minute); err != nil {
		log.Println(err)
	}
}

// A deduplicação descarta sensor_id + seq repetidos dentro de uma janela.
func ExampleDedup() {
	d := client.NewDedup(0)
	fmt.Println(d.Accept("sensor-a", 1))
	fmt.Println(d.Accept("sensor-a", 1))
	fmt.Println(d.Accept("sensor-b", 1))
	// Output:
	// true
	// false
	// true
}
