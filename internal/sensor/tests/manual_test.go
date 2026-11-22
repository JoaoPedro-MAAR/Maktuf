package sensor_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/broker"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/sensor"
)

// syncBuffer é um bytes.Buffer seguro para o consumidor escrever enquanto o
// teste lê.
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

func newManual(t *testing.T, addr, dir string) *sensor.Manual {
	t.Helper()
	m, err := sensor.NewManual(sensor.ManualConfig{
		Addr:           addr,
		SensorID:       "demo",
		BufferPath:     filepath.Join(dir, "demo.buf"),
		PublishTimeout: time.Second,
		Reconnect:      fastReconnect,
	})
	if err != nil {
		t.Fatalf("NewManual: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func runManual(t *testing.T, m *sensor.Manual, script string) string {
	t.Helper()
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := m.Run(ctx, strings.NewReader(script), &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out.String()
}

func expectContains(t *testing.T, what, text string, parts ...string) {
	t.Helper()
	for _, p := range parts {
		if !strings.Contains(text, p) {
			t.Fatalf("%s sem %q:\n%s", what, p, text)
		}
	}
}

// O sensor manual publica, faz rajada e dorme; o consumidor no modo de
// apresentação mostra cada leitura e a transição de estado.
func TestManualSensorAndReadableConsumer(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	display := &syncBuffer{}
	startConsumerWith(t, consumer.Config{Addr: addr, Display: display})

	out := runManual(t, newManual(t, addr, t.TempDir()), "ajuda\n23.5\nrajada 3 20\ndormir 1s\n24,5\nstatus\nsair\n")
	expectContains(t, "saida do sensor", out,
		"comandos:",
		"seq=1 PERSISTED",
		"rajada enviada",
		"SLEEP(1s) enviado",
		"seq=5 PERSISTED",
		"ultimo seq: 5 | pendentes no buffer: 0",
	)

	waitFor(t, "consumidor mostrar a ultima leitura", func() bool { return strings.Contains(display.String(), "seq 5 ") })
	expectContains(t, "saida do consumidor", display.String(),
		"demo           seq 1        23.50 C",
		"agora esta DORMINDO",
		"agora esta ONLINE",
		"seq 5        24.50 C",
	)
}

// Sem broker, as leituras ficam no buffer; ao conectar, são reenviadas.
func TestManualSensorBuffersWhileBrokerIsDown(t *testing.T) {
	addr := freeAddr(t)
	dir := t.TempDir()

	out := runManual(t, newManual(t, addr, dir), "21\n22\npendentes\nsair\n")
	expectContains(t, "saida sem broker", out,
		"sem conexao",
		"seq=1 guardada no buffer (desconectado); pendentes: 1",
		"seq=2 guardada no buffer (desconectado); pendentes: 2",
		"seq=2  22.00",
	)

	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)
	out = runManual(t, newManual(t, addr, dir), "status\nsair\n")
	expectContains(t, "saida apos o broker voltar", out,
		"buffer reaberto: ultimo seq 2, 2 pendentes",
		"reenviando 2 leituras pendentes",
	)
	waitFor(t, "consumidor receber o backlog", func() bool { return cons.Stats().Received == 2 })
}

// "desconectar" simula uma queda: as leituras ficam no buffer até "conectar",
// que reenvia tudo em ordem.
func TestManualDisconnectKeepsReadingsUntilConnect(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumer(t, addr)

	out := runManual(t, newManual(t, addr, t.TempDir()), "1\ndesconectar\n2\n3\nconectar\nstatus\nsair\n")
	expectContains(t, "saida", out,
		"seq=1 PERSISTED",
		"seq=2 guardada no buffer (desconectado); pendentes: 1",
		"seq=3 guardada no buffer (desconectado); pendentes: 2",
		"reenviando 2 leituras pendentes",
	)
	waitFor(t, "consumidor receber as 3", func() bool { return cons.Stats().Received == 3 })
	if cs := cons.Stats(); cs.GapsUnmarked != 0 {
		t.Fatalf("lacunas: %+v", cs)
	}
}

// Com o broker fora depois de conectar, "conectar" não diz "ja conectado":
// avisa que a biblioteca está reconectando e a leitura fica no buffer.
func TestManualReportsReconnectingWhenBrokerIsGone(t *testing.T) {
	addr := freeAddr(t)
	b := startBroker(t, addr, broker.Config{})
	startConsumer(t, addr)
	m := newManual(t, addr, t.TempDir())

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	in, feed := io.Pipe()
	var out syncBuffer
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx, in, &out) }()
	say := func(line string) { _, _ = io.WriteString(feed, line+"\n") }

	say("20")
	waitFor(t, "primeira leitura", func() bool { return strings.Contains(out.String(), "seq=1 PERSISTED") })
	b.stop(t)
	time.Sleep(200 * time.Millisecond)
	say("21")
	say("conectar")
	say("sair")
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	expectContains(t, "saida", out.String(),
		"seq=2 sem PUBACK por enquanto",
		"(sem conexao: reconectando a "+addr+")",
		"tentando reconectar a "+addr+" sozinha",
	)
	if strings.Contains(out.String(), "ja conectado") {
		t.Fatalf("disse 'ja conectado' com o broker fora:\n%s", out.String())
	}
}

// "publicar" usa a mesma conexão para outro tópico, num fluxo próprio: o seq
// não colide com o do buffer e a leitura chega ao consumidor desse tópico.
func TestManualPublishToAnotherTopic(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	temp := startConsumer(t, addr)
	humidity := startConsumerWith(t, consumer.Config{
		Addr: addr, ClientID: "umidade", Filters: []string{"sensores/+/umidade"},
	})

	out := runManual(t, newManual(t, addr, t.TempDir()),
		"20\npublicar sensores/demo/umidade 55\npublicar sensores/demo/umidade 56\n21\npublicar sensores/+/x 1\npublicar so-um-arg\nsair\n")
	expectContains(t, "saida", out,
		"seq=1 PERSISTED",
		"sensores/demo/umidade (fluxo demo.umidade, seq=",
		"PERSISTED",
		"seq=2 PERSISTED",
		`topico invalido "sensores/+/x"`,
		"uso: publicar <topico> <temperatura>",
	)
	waitFor(t, "consumidor de umidade", func() bool { return humidity.Stats().Received == 2 })
	waitFor(t, "consumidor de temperatura", func() bool { return temp.Stats().Received == 2 })
	if hs := humidity.Stats(); hs.GapsUnmarked != 0 || hs.Sensors != 1 {
		t.Fatalf("umidade: %+v", hs)
	}
}

func TestManualCustomTopicFlag(t *testing.T) {
	addr := freeAddr(t)
	startBroker(t, addr, broker.Config{})
	cons := startConsumerWith(t, consumer.Config{Addr: addr, Filters: []string{"estacao/norte/{id}"}})
	_ = cons
	m, err := sensor.NewManual(sensor.ManualConfig{
		Addr: addr, SensorID: "demo", BufferPath: filepath.Join(t.TempDir(), "d.buf"),
		Topic: "estacao/norte/" + sensor.TopicIDPlaceholder, PublishTimeout: time.Second, Reconnect: fastReconnect,
	})
	if err != nil {
		t.Fatalf("NewManual: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if got := m.State().Topic; got != "estacao/norte/demo" {
		t.Fatalf("topico = %q", got)
	}
	if _, err := sensor.NewManual(sensor.ManualConfig{
		Addr: addr, SensorID: "demo", BufferPath: filepath.Join(t.TempDir(), "x.buf"), Topic: "a/+/b",
	}); err == nil {
		t.Fatalf("topico com curinga aceito")
	}
}

func TestManualRejectsBadCommands(t *testing.T) {
	out := runManual(t, newManual(t, freeAddr(t), t.TempDir()), "voar\ndormir\ndormir xyz\nrajada -1\nsair\n")
	expectContains(t, "saida", out,
		`comando desconhecido "voar"`,
		"uso: dormir <duracao>",
		`duracao invalida "xyz"`,
		`quantidade invalida "-1"`,
	)
}
