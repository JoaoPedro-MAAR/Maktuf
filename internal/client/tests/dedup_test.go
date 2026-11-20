package client_test

import (
	"sync"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
)

func TestDedupDiscardsDuplicatesBySensorAndSeq(t *testing.T) {
	d := client.NewDedup(10)

	if !d.Accept("sensor-a", 1) {
		t.Fatalf("primeira leitura deveria ser aceita")
	}
	if d.Accept("sensor-a", 1) {
		t.Fatalf("duplicata deveria ser descartada")
	}
	if !d.Accept("sensor-b", 1) {
		t.Fatalf("mesmo seq de outro sensor deveria ser aceito")
	}
	if !d.Accept("sensor-a", 2) {
		t.Fatalf("seq seguinte deveria ser aceito")
	}
}

func TestDedupEvictsOldestKeyWhenWindowIsFull(t *testing.T) {
	d := client.NewDedup(2)

	if !d.Accept("a", 1) || !d.Accept("b", 1) {
		t.Fatalf("primeiras chaves deveriam ser aceitas")
	}
	if !d.Accept("c", 1) {
		t.Fatalf("chave nova deveria ser aceita")
	}
	if !d.Accept("a", 1) {
		t.Fatalf("chave mais antiga deveria ter saido da janela")
	}
	if d.Accept("c", 1) {
		t.Fatalf("chave recente deveria continuar deduplicada")
	}
}

func TestDedupAcceptReadingIgnoresStatusStream(t *testing.T) {
	d := client.NewDedup(10)

	status := client.Message{SensorID: "sensor-a", Seq: 1, Status: client.SensorStateOnline}
	first := d.AcceptReading(status)
	second := d.AcceptReading(status)
	if !first || !second {
		t.Fatalf("transicoes de status pertencem a outro fluxo e nao devem ser deduplicadas")
	}

	reading := client.Message{SensorID: "sensor-a", Seq: 1}
	if !d.AcceptReading(reading) {
		t.Fatalf("leitura deveria ser aceita")
	}
	if d.AcceptReading(reading) {
		t.Fatalf("leitura duplicada deveria ser descartada")
	}
}

func TestDedupIsConcurrentSafe(t *testing.T) {
	d := client.NewDedup(1000)

	const workers = 8
	var accepted int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d.Accept("sensor-a", 7) {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if accepted != 1 {
		t.Fatalf("chave concorrente aceita %d vezes, esperado 1", accepted)
	}
}
