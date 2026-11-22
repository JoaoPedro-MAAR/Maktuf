package consumer_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
)

const waitTimeout = 5 * time.Second

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func startBroker(t *testing.T, cfg broker.Config) string {
	t.Helper()
	cfg.Addr = "127.0.0.1:0"
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := broker.NewServer(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return srv.Addr().String()
}

// session roda o consumidor manual e permite digitar comandos.
type session struct {
	out  *syncBuffer
	feed *io.PipeWriter
	done chan error
}

func startManual(t *testing.T, addr string) *session {
	t.Helper()
	m, err := consumer.NewManual(consumer.ManualConfig{Addr: addr, ClientID: "painel", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewManual: %v", err)
	}
	in, feed := io.Pipe()
	s := &session{out: &syncBuffer{}, feed: feed, done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { s.done <- m.Run(ctx, in, s.out) }()
	t.Cleanup(func() {
		cancel()
		_ = feed.Close()
		<-s.done
		_ = m.Close()
	})
	s.waitFor(t, "conexao", "conectado")
	return s
}

func (s *session) say(line string) { _, _ = io.WriteString(s.feed, line+"\n") }

func (s *session) waitFor(t *testing.T, what, text string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !strings.Contains(s.out.String(), text) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout aguardando %s (%q):\n%s", what, text, s.out.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func publish(t *testing.T, sensor *client.Client, seq uint64) client.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	status, err := sensor.Publish(ctx, "sensores/sala-01/temperatura", client.Reading{SensorID: "sala-01", Seq: seq, Temperature: 20})
	if err != nil {
		t.Fatalf("Publish %d: %v", seq, err)
	}
	return status
}

func connectSensor(t *testing.T, addr string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	c, err := client.Connect(ctx, addr, client.RoleSensor, "sala-01")
	if err != nil {
		t.Fatalf("Connect sensor: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Inscrever recebe; cancelar faz o sensor voltar a ver NO_SUBSCRIBERS (RF-03,
// RF-04, RF-07).
func TestManualConsumerSubscribeAndCancel(t *testing.T) {
	addr := startBroker(t, broker.Config{})
	s := startManual(t, addr)
	sensor := connectSensor(t, addr)

	if got := publish(t, sensor, 1); got != client.StatusNoSubscribers {
		t.Fatalf("antes de inscrever = %s", got)
	}
	s.say("inscrever sensores/+/temperatura")
	s.waitFor(t, "inscricao", "inscrito em sensores/+/temperatura")
	if got := publish(t, sensor, 2); got != client.StatusPersisted {
		t.Fatalf("depois de inscrever = %s", got)
	}
	s.waitFor(t, "leitura", "sala-01        seq 2 ")

	s.say("inscricoes")
	s.waitFor(t, "lista", "  sensores/+/temperatura\n")
	s.say("cancelar sensores/+/temperatura")
	s.waitFor(t, "cancelamento", "inscricao em sensores/+/temperatura cancelada")
	if got := publish(t, sensor, 3); got != client.StatusNoSubscribers {
		t.Fatalf("depois de cancelar = %s", got)
	}
	s.say("status")
	s.waitFor(t, "status", "recebidas: 1")
}

// Pausado, o consumidor deixa a fila crescer no broker; com retenção pequena,
// ao retomar aparece o aviso de perda por retenção (RF-36, RF-37).
func TestManualConsumerPauseOverflowsRetention(t *testing.T) {
	addr := startBroker(t, broker.Config{SubscriberQueueLimit: 3})
	s := startManual(t, addr)
	sensor := connectSensor(t, addr)

	s.say("inscrever sensores/sala-01/temperatura")
	s.waitFor(t, "inscricao", "inscrito em")
	publish(t, sensor, 1)
	s.waitFor(t, "primeira leitura", "seq 1 ")

	s.say("pausar")
	s.waitFor(t, "pausa", "pausado:")
	// 64 ficam no canal da biblioteca; o resto passa da retenção (3) da fila.
	for seq := uint64(2); seq <= 90; seq++ {
		publish(t, sensor, seq)
	}
	s.say("retomar")
	s.waitFor(t, "aviso de retencao", "perdida(s) por retencao da fila (GAP_BEFORE)")
	s.waitFor(t, "ultima leitura", "seq 90 ")
}

func TestManualConsumerRejectsBadInput(t *testing.T) {
	s := startManual(t, startBroker(t, broker.Config{}))
	s.say("voar")
	s.waitFor(t, "desconhecido", `comando desconhecido "voar"`)
	s.say("inscrever")
	s.waitFor(t, "uso", "uso: inscrever <filtro>")
	s.say("inscrever sensores/#")
	s.waitFor(t, "filtro invalido", `filtro invalido "sensores/#"`)
	s.say("inscrever sala-01")
	s.waitFor(t, "aviso de prefixo", `"sala-01" nao casa com nenhum deles`)
	// O filtro inválido não derrubou a conexão: uma inscrição válida funciona.
	s.say("inscrever sensores/+/status")
	s.waitFor(t, "inscricao valida", "inscrito em sensores/+/status")
}
