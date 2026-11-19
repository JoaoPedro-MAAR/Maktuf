package storage_test

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

var t0 = time.UnixMilli(1_700_000_000_000)

func bufferPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "sensor-a.wal")
}

func open(t *testing.T, path string, max int) *storage.FileBuffer {
	t.Helper()
	b, err := storage.OpenSensorBuffer(path, storage.BufferOptions{
		MaxReadings: max,
		SensorID:    "sensor-a",
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("OpenSensorBuffer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func reopen(t *testing.T, b *storage.FileBuffer, path string, max int) *storage.FileBuffer {
	t.Helper()
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return open(t, path, max)
}

func appendN(t *testing.T, b storage.Buffer, n int) []storage.Reading {
	t.Helper()
	out := make([]storage.Reading, 0, n)
	for i := 0; i < n; i++ {
		r, err := b.Append(t0.Add(time.Duration(len(out))*time.Second), float32(20+i))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func ack(t *testing.T, b storage.Buffer, seq uint64) {
	t.Helper()
	ok, err := b.Ack(seq)
	if err != nil {
		t.Fatalf("Ack(%d): %v", seq, err)
	}
	if !ok {
		t.Fatalf("Ack(%d) nao encontrou a leitura", seq)
	}
}

func seqs(rs []storage.Reading) []uint64 {
	out := make([]uint64, len(rs))
	for i, r := range rs {
		out[i] = r.Seq
	}
	return out
}

func expectPending(t *testing.T, b storage.Buffer, want ...uint64) {
	t.Helper()
	got := seqs(b.Pending())
	if len(got) != len(want) {
		t.Fatalf("pendentes = %v, esperado %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("pendentes = %v, esperado %v", got, want)
		}
	}
}

func TestOpenRejectsInvalidLimit(t *testing.T) {
	for _, max := range []int{0, -1} {
		_, err := storage.OpenSensorBuffer(bufferPath(t), storage.BufferOptions{MaxReadings: max})
		if !errors.Is(err, storage.ErrInvalidConfig) {
			t.Errorf("MaxReadings=%d: esperava ErrInvalidConfig, obtido %v", max, err)
		}
	}
}

func TestAppendAssignsIncreasingSeq(t *testing.T) {
	b := open(t, bufferPath(t), 10)
	rs := appendN(t, b, 3)
	for i, r := range rs {
		if r.Seq != uint64(i+1) {
			t.Fatalf("seq = %d, esperado %d", r.Seq, i+1)
		}
	}
	expectPending(t, b, 1, 2, 3)
}

// RF-23, RNF-12, CE-02: leituras sem ACK sobrevivem a fechar e reabrir.
func TestPendingSurvivesReopen(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	written := appendN(t, b, 3)

	b = reopen(t, b, path, 10)

	got := b.Pending()
	if len(got) != len(written) {
		t.Fatalf("pendentes = %v, esperado %v", seqs(got), seqs(written))
	}
	for i := range got {
		w, g := written[i], got[i]
		if g.Seq != w.Seq || g.Temperature != w.Temperature || !g.MeasuredAt.Equal(w.MeasuredAt) {
			t.Fatalf("leitura %d = %+v, esperado %+v", i, g, w)
		}
	}
}

// RF-24: Ack remove a leitura de forma durável; Ack desconhecido não tem efeito.
func TestAckRemovesReading(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	appendN(t, b, 3)

	ack(t, b, 2)
	expectPending(t, b, 1, 3)

	if ok, err := b.Ack(2); err != nil || ok {
		t.Fatalf("Ack repetido: ok=%v err=%v, esperado false/nil", ok, err)
	}
	if ok, err := b.Ack(99); err != nil || ok {
		t.Fatalf("Ack desconhecido: ok=%v err=%v, esperado false/nil", ok, err)
	}

	b = reopen(t, b, path, 10)
	expectPending(t, b, 1, 3)
}

// RF-25, CE-03: o reenvio segue seq crescente, mesmo após ACKs fora de ordem
// e reabertura.
func TestPendingIsOrderedBySeq(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 20)
	appendN(t, b, 8)
	for _, seq := range []uint64{7, 2, 5} {
		ack(t, b, seq)
	}
	appendN(t, b, 2)

	want := []uint64{1, 3, 4, 6, 8, 9, 10}
	expectPending(t, b, want...)
	b = reopen(t, b, path, 20)
	expectPending(t, b, want...)
}

// A sequência continua após reinício mesmo sem leituras pendentes; voltar a
// seq 1 faria o broker tratar as novas leituras como reenvio e não entregá-las.
func TestSeqContinuesAfterReopenWithEmptyBuffer(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	for _, r := range appendN(t, b, 3) {
		ack(t, b, r.Seq)
	}

	// Duas reaberturas: a primeira compacta o arquivo sem leituras pendentes;
	// a segunda só conhece o seq pelo estado gravado na compactação.
	b = reopen(t, b, path, 10)
	b = reopen(t, b, path, 10)
	expectPending(t, b)
	if r := appendN(t, b, 1)[0]; r.Seq != 4 {
		t.Fatalf("seq apos reinicio = %d, esperado 4", r.Seq)
	}
}

// RF-26, CE-04, CT-07: buffer cheio descarta a mais antiga e registra
// contador e sequência, inclusive após reinício.
func TestOverflowDropsOldest(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 3)
	appendN(t, b, 5)

	expectPending(t, b, 3, 4, 5)
	want := storage.BufferStats{Pending: 3, LastSeq: 5, Dropped: 2, LastDroppedSeq: 2}
	if got := b.Stats(); got != want {
		t.Fatalf("Stats = %+v, esperado %+v", got, want)
	}

	b = reopen(t, b, path, 3)
	b = reopen(t, b, path, 3)
	expectPending(t, b, 3, 4, 5)
	if got := b.Stats(); got != want {
		t.Fatalf("Stats apos reinicio = %+v, esperado %+v", got, want)
	}

	appendN(t, b, 1)
	if got := b.Stats(); got.Dropped != 3 || got.LastDroppedSeq != 3 {
		t.Fatalf("Stats = %+v, esperado Dropped=3 LastDroppedSeq=3", got)
	}
}

func TestReopenWithSmallerLimitDropsOldest(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	appendN(t, b, 5)

	b = reopen(t, b, path, 2)
	expectPending(t, b, 4, 5)
	if got := b.Stats(); got.Dropped != 3 || got.LastDroppedSeq != 3 {
		t.Fatalf("Stats = %+v, esperado Dropped=3 LastDroppedSeq=3", got)
	}
}

// Uma escrita interrompida deixa bytes parciais no fim do arquivo. A cauda é
// descartada e o estado confirmado é preservado.
func TestTornTailIsDiscarded(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	appendN(t, b, 2)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("abrir arquivo: %v", err)
	}
	if _, err := f.Write([]byte{1, 0, 0, 0, 0, 0, 0, 0, 3, 0xFF}); err != nil {
		t.Fatalf("escrever cauda: %v", err)
	}
	_ = f.Close()

	b = open(t, path, 10)
	expectPending(t, b, 1, 2)
	if r := appendN(t, b, 1)[0]; r.Seq != 3 {
		t.Fatalf("seq = %d, esperado 3", r.Seq)
	}
}

func TestCorruptionBeforeTailIsReported(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 10)
	appendN(t, b, 6)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ler arquivo: %v", err)
	}
	data[len(data)/3] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("gravar arquivo: %v", err)
	}

	_, err = storage.OpenSensorBuffer(path, storage.BufferOptions{
		MaxReadings: 10,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if !errors.Is(err, storage.ErrCorrupted) {
		t.Fatalf("esperava ErrCorrupted, obtido %v", err)
	}
}

// Ciclos longos de append/ack não fazem o arquivo crescer sem limite.
func TestCompactionBoundsFileSize(t *testing.T) {
	// Sem compactação, 300 ciclos gerariam ~15 KB de registros.
	const cycles = 300
	path := bufferPath(t)
	b := open(t, path, 10)
	for i := 0; i < cycles; i++ {
		r := appendN(t, b, 1)[0]
		ack(t, b, r.Seq)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() > 4<<10 {
		t.Fatalf("arquivo com %d bytes apos compactacao", info.Size())
	}
	if got := b.Stats().LastSeq; got != cycles {
		t.Fatalf("LastSeq = %d, esperado %d", got, cycles)
	}

	b = reopen(t, b, path, 10)
	if r := appendN(t, b, 1)[0]; r.Seq != cycles+1 {
		t.Fatalf("seq = %d, esperado %d", r.Seq, cycles+1)
	}
}

func TestClosedBufferRejectsOperations(t *testing.T) {
	b := open(t, bufferPath(t), 10)
	appendN(t, b, 1)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close repetido: %v", err)
	}
	if _, err := b.Append(t0, 1); !errors.Is(err, storage.ErrBufferClosed) {
		t.Fatalf("Append apos Close: %v", err)
	}
	if _, err := b.Ack(1); !errors.Is(err, storage.ErrBufferClosed) {
		t.Fatalf("Ack apos Close: %v", err)
	}
}

func TestConcurrentAppendAndAck(t *testing.T) {
	path := bufferPath(t)
	b := open(t, path, 1000)

	const writers, perWriter = 4, 50
	acks := make(chan uint64, writers*perWriter)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				r, err := b.Append(t0, 1)
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				if r.Seq%2 == 0 {
					acks <- r.Seq
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writers*perWriter/2; i++ {
			seq := <-acks
			if _, err := b.Ack(seq); err != nil {
				t.Errorf("Ack: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	b = reopen(t, b, path, 1000)
	got := seqs(b.Pending())
	if len(got) != writers*perWriter/2 {
		t.Fatalf("%d pendentes, esperado %d", len(got), writers*perWriter/2)
	}
	for i, seq := range got {
		if seq != uint64(2*i+1) {
			t.Fatalf("pendentes fora do esperado: %v", got)
		}
	}
}

const (
	crashChildEnv  = "MAKTUF_BUFFER_CRASH_PATH"
	crashChildDone = "filho-gravou"
)

// RF-23, RNF-12, CE-02: um processo morto com SIGKILL, sem Close e sem rodar
// defers, não perde leituras pendentes nem ACKs já confirmados. O teste executa
// o próprio binário como processo filho.
func TestPendingSurvivesProcessKill(t *testing.T) {
	if path := os.Getenv(crashChildEnv); path != "" {
		crashChild(path)
		return
	}

	path := bufferPath(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestPendingSurvivesProcessKill$")
	cmd.Env = append(os.Environ(), crashChildEnv+"="+path)
	out, err := cmd.Output()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.Success() {
		t.Fatalf("o filho deveria morrer abruptamente; err=%v saida=%q", err, out)
	}
	if !strings.Contains(string(out), crashChildDone) {
		t.Fatalf("o filho morreu antes de gravar; saida=%q", out)
	}

	b := open(t, path, 10)
	expectPending(t, b, 1, 3, 4, 5)
	if r := appendN(t, b, 1)[0]; r.Seq != 6 {
		t.Fatalf("seq apos queda = %d, esperado 6", r.Seq)
	}
}

func crashChild(path string) {
	b, err := storage.OpenSensorBuffer(path, storage.BufferOptions{
		MaxReadings: 10,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		fmt.Println("erro:", err)
		os.Exit(2)
	}
	for i := 0; i < 5; i++ {
		if _, err := b.Append(t0, float32(i)); err != nil {
			fmt.Println("erro:", err)
			os.Exit(2)
		}
	}
	if ok, err := b.Ack(2); err != nil || !ok {
		fmt.Println("erro no ack:", ok, err)
		os.Exit(2)
	}
	fmt.Println(crashChildDone)

	self, err := os.FindProcess(os.Getpid())
	if err == nil {
		_ = self.Kill()
	}
	select {}
}

func TestReadingAge(t *testing.T) {
	r := storage.Reading{MeasuredAt: t0}
	if got := r.Age(t0.Add(90 * time.Second)); got != 90*time.Second {
		t.Fatalf("Age = %v, esperado 90s", got)
	}
	if got := r.Age(t0.Add(-time.Second)); got != 0 {
		t.Fatalf("Age com relogio atrasado = %v, esperado 0", got)
	}
}
