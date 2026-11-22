package storage_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

const (
	logMagic   = "TEST"
	logVersion = 1
)

func openLog(t *testing.T, path string) (*storage.RecordLog, [][]byte) {
	t.Helper()
	l, recs, err := storage.OpenRecordLog(path, logMagic, logVersion)
	if err != nil {
		t.Fatalf("OpenRecordLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, recs
}

func logPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func appendAll(t *testing.T, l *storage.RecordLog, recs ...string) {
	t.Helper()
	for _, r := range recs {
		if err := l.Append([]byte(r)); err != nil {
			t.Fatalf("Append(%q): %v", r, err)
		}
	}
}

func strs(recs [][]byte) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = string(r)
	}
	return out
}

func TestRecordLogReopenReturnsRecordsInOrder(t *testing.T) {
	path := logPath(t)
	l, recs := openLog(t, path)
	if len(recs) != 0 {
		t.Fatalf("log novo com %d registros", len(recs))
	}
	appendAll(t, l, "um", "", "tres")
	_ = l.Close()

	l, recs = openLog(t, path)
	if got := strs(recs); !reflect.DeepEqual(got, []string{"um", "", "tres"}) {
		t.Fatalf("registros = %q", got)
	}
	if l.Records() != 3 {
		t.Fatalf("Records = %d, esperado 3", l.Records())
	}
}

func TestRecordLogRewriteReplacesContent(t *testing.T) {
	path := logPath(t)
	l, _ := openLog(t, path)
	appendAll(t, l, "a", "b", "c")
	if err := l.Rewrite([][]byte{[]byte("x")}); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	appendAll(t, l, "y")
	_ = l.Close()

	_, recs := openLog(t, path)
	if got := strs(recs); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("registros = %q", got)
	}
}

// Escrita interrompida no último registro é descartada e o log continua
// aceitando gravações.
func TestRecordLogDiscardsTruncatedTail(t *testing.T) {
	path := logPath(t)
	l, _ := openLog(t, path)
	appendAll(t, l, "inteiro", "cortado")
	_ = l.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	l, recs := openLog(t, path)
	if got := strs(recs); !reflect.DeepEqual(got, []string{"inteiro"}) {
		t.Fatalf("registros = %q", got)
	}
	appendAll(t, l, "novo")
	_ = l.Close()

	_, recs = openLog(t, path)
	if got := strs(recs); !reflect.DeepEqual(got, []string{"inteiro", "novo"}) {
		t.Fatalf("registros = %q", got)
	}
}

func TestRecordLogDetectsCorruptionBeforeTail(t *testing.T) {
	path := logPath(t)
	l, _ := openLog(t, path)
	appendAll(t, l, "primeiro", "segundo")
	_ = l.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// cabeçalho (5) + length (4) + crc (4): primeiro byte do primeiro registro.
	data[13] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := storage.OpenRecordLog(path, logMagic, logVersion); !errors.Is(err, storage.ErrCorrupted) {
		t.Fatalf("erro = %v, esperado ErrCorrupted", err)
	}
}

func TestRecordLogRejectsWrongHeader(t *testing.T) {
	path := logPath(t)
	l, _ := openLog(t, path)
	_ = l.Close()

	if _, _, err := storage.OpenRecordLog(path, "XXXX", logVersion); !errors.Is(err, storage.ErrCorrupted) {
		t.Fatalf("magic errado: erro = %v", err)
	}
	if _, _, err := storage.OpenRecordLog(path, logMagic, logVersion+1); !errors.Is(err, storage.ErrCorrupted) {
		t.Fatalf("versao errada: erro = %v", err)
	}
	if _, _, err := storage.OpenRecordLog(path, "AB", logVersion); !errors.Is(err, storage.ErrInvalidConfig) {
		t.Fatalf("magic curto: erro = %v", err)
	}
}

func TestRecordLogRefusesAfterClose(t *testing.T) {
	l, _ := openLog(t, logPath(t))
	_ = l.Close()
	if err := l.Append([]byte("x")); !errors.Is(err, storage.ErrLogClosed) {
		t.Fatalf("erro = %v, esperado ErrLogClosed", err)
	}
}

func TestRecordLogBufferedRecordsSurviveReopen(t *testing.T) {
	path := logPath(t)
	l, _, err := storage.OpenRecordLog(path, logMagic, logVersion, storage.WithUnsyncedTail())
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("leitura")); err != nil {
		t.Fatal(err)
	}
	if err := l.AppendBuffered([]byte("ack")); err != nil {
		t.Fatal(err)
	}
	// Sem Close: simula a queda do processo, com o registro só no cache do SO.
	_, recs, err := storage.OpenRecordLog(path, logMagic, logVersion, storage.WithUnsyncedTail())
	if err != nil {
		t.Fatal(err)
	}
	if got := strs(recs); !reflect.DeepEqual(got, []string{"leitura", "ack"}) {
		t.Fatalf("registros = %q", got)
	}
	_ = l.Close()
}

// Uma queda da máquina pode deixar a cauda não sincronizada corrompida ou
// zerada, com registros depois dela; com WithUnsyncedTail, tudo a partir do
// primeiro registro inválido é descartado.
func TestRecordLogUnsyncedTailIsDiscarded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(data []byte, at int) []byte
	}{
		{"crc invalido seguido de registro", func(data []byte, at int) []byte {
			data[at+8] ^= 0xFF
			return data
		}},
		{"cauda zerada seguida de registro", func(data []byte, at int) []byte {
			for i := at; i < at+8+len("ack-1"); i++ {
				data[i] = 0
			}
			return data
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := logPath(t)
			l, _, err := storage.OpenRecordLog(path, logMagic, logVersion, storage.WithUnsyncedTail())
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Append([]byte("leitura")); err != nil {
				t.Fatal(err)
			}
			for _, r := range []string{"ack-1", "ack-2"} {
				if err := l.AppendBuffered([]byte(r)); err != nil {
					t.Fatal(err)
				}
			}
			_ = l.Close()

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// cabeçalho (5) + registro "leitura" (8 + 7): início de "ack-1".
			data = tc.corrupt(data, 5+8+len("leitura"))
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}

			l, recs, err := storage.OpenRecordLog(path, logMagic, logVersion, storage.WithUnsyncedTail())
			if err != nil {
				t.Fatalf("OpenRecordLog: %v", err)
			}
			defer func() { _ = l.Close() }()
			if got := strs(recs); !reflect.DeepEqual(got, []string{"leitura"}) {
				t.Fatalf("registros = %q", got)
			}
			if err := l.Append([]byte("novo")); err != nil {
				t.Fatal(err)
			}
			_ = l.Close()
			_, recs, err = storage.OpenRecordLog(path, logMagic, logVersion, storage.WithUnsyncedTail())
			if err != nil {
				t.Fatal(err)
			}
			if got := strs(recs); !reflect.DeepEqual(got, []string{"leitura", "novo"}) {
				t.Fatalf("depois da reabertura: registros = %q", got)
			}
		})
	}
}

func TestRecordLogUnsyncedTailRejectsEmptyRecord(t *testing.T) {
	l, _, err := storage.OpenRecordLog(logPath(t), logMagic, logVersion, storage.WithUnsyncedTail())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if err := l.AppendBuffered(nil); !errors.Is(err, storage.ErrInvalidConfig) {
		t.Fatalf("erro = %v, esperado ErrInvalidConfig", err)
	}
}
