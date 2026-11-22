package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// ErrLogClosed indica uso de um RecordLog já fechado.
var ErrLogClosed = errors.New("storage: log fechado")

// Formato do RecordLog (WAL append-only de registros de tamanho variável):
//
//	cabeçalho: magic (4 bytes) + versão (uint8)
//	registros, big-endian: length:uint32 | crc32:uint32 | data[length]
//
// O CRC (IEEE) cobre data. Cada Append grava exatamente um registro, então só
// o último registro pode estar incompleto: um registro inválido que termina no
// fim do arquivo é uma escrita interrompida e é descartado na abertura. Um
// registro inválido seguido de outros indica corrupção.
const (
	logMagicSize      = 4
	logRecordOverhead = 4 + 4
)

// RecordLog é um WAL genérico com fsync a cada gravação (Append). Com
// AppendBuffered, o registro é gravado sem fsync e só chega ao disco no próximo
// fsync do arquivo. O conteúdo dos registros é opaco para o storage. Pode ser
// usado por várias goroutines.
type RecordLog struct {
	path    string
	header  []byte
	mu      sync.Mutex
	file    *os.File
	records int
	err     error
	// unsynced indica gravações sem fsync desde o último fsync.
	unsynced bool
	// tolerant trata qualquer registro inválido como o fim do log (ver
	// WithUnsyncedTail).
	tolerant bool
}

// Option ajusta a abertura de um RecordLog.
type Option func(*RecordLog)

// WithUnsyncedTail serve a logs que usam AppendBuffered. Uma queda da máquina
// pode deixar vários registros não sincronizados incompletos ou zerados no fim
// do arquivo. Como cada fsync grava tudo o que veio antes, um registro inválido
// só pode estar nessa cauda: a abertura descarta o arquivo a partir do primeiro
// registro inválido (incluindo os de tamanho zero), em vez de acusar corrupção.
func WithUnsyncedTail() Option {
	return func(l *RecordLog) { l.tolerant = true }
}

// OpenRecordLog abre (ou cria) o log em path e devolve os registros válidos,
// na ordem em que foram gravados. magic deve ter 4 bytes.
func OpenRecordLog(path, magic string, version byte, opts ...Option) (*RecordLog, [][]byte, error) {
	if len(magic) != logMagicSize {
		return nil, nil, fmt.Errorf("%w: magic deve ter %d bytes", ErrInvalidConfig, logMagicSize)
	}
	l := &RecordLog{path: path, header: append([]byte(magic), version)}
	for _, opt := range opts {
		opt(l)
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := l.create(); err != nil {
			return nil, nil, err
		}
		return l, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("storage: ler log: %w", err)
	}

	recs, valid, err := l.parse(data)
	if err != nil {
		return nil, nil, err
	}
	if valid < len(data) {
		if err := truncateSync(path, int64(valid)); err != nil {
			return nil, nil, fmt.Errorf("storage: descartar cauda do log: %w", err)
		}
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: abrir log: %w", err)
	}
	l.file = f
	l.records = len(recs)
	return l, recs, nil
}

// parse devolve os registros e o tamanho do prefixo válido do arquivo.
func (l *RecordLog) parse(data []byte) ([][]byte, int, error) {
	hs := len(l.header)
	if len(data) < hs || !bytes.Equal(data[:logMagicSize], l.header[:logMagicSize]) {
		return nil, 0, fmt.Errorf("%w: cabecalho invalido", ErrCorrupted)
	}
	if v := data[logMagicSize]; v != l.header[logMagicSize] {
		return nil, 0, fmt.Errorf("%w: versao %d nao suportada", ErrCorrupted, v)
	}

	var recs [][]byte
	off := hs
	for off < len(data) {
		rest := data[off:]
		if len(rest) < logRecordOverhead {
			break
		}
		n := int(binary.BigEndian.Uint32(rest[0:4]))
		sum := binary.BigEndian.Uint32(rest[4:8])
		end := logRecordOverhead + n
		if n > len(rest)-logRecordOverhead || (l.tolerant && n == 0) {
			break
		}
		body := rest[logRecordOverhead:end]
		if crc32.ChecksumIEEE(body) != sum {
			if l.tolerant || end == len(rest) {
				break
			}
			return nil, 0, fmt.Errorf("%w: registro invalido no byte %d", ErrCorrupted, off)
		}
		recs = append(recs, append([]byte(nil), body...))
		off += end
	}
	return recs, off, nil
}

func (l *RecordLog) create() error {
	if err := writeFileSync(l.path, l.header); err != nil {
		return fmt.Errorf("storage: criar log: %w", err)
	}
	if err := syncDir(filepath.Dir(l.path)); err != nil {
		return fmt.Errorf("storage: fsync do diretorio: %w", err)
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("storage: abrir log: %w", err)
	}
	l.file = f
	return nil
}

func frame(buf, rec []byte) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec)))
	buf = binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(rec))
	return append(buf, rec...)
}

// Append grava o registro e força o fsync antes de retornar; o fsync também
// leva ao disco os registros anteriores gravados com AppendBuffered. Depois de
// uma falha de escrita o estado do arquivo é incerto, então o log passa a
// recusar operações.
func (l *RecordLog) Append(rec []byte) error {
	return l.append(rec, true)
}

// AppendBuffered grava o registro sem fsync. Ele sobrevive à queda do
// processo (fica no cache do SO), mas pode se perder numa
// queda da máquina até o próximo Append, Rewrite ou Close. Use com
// WithUnsyncedTail.
func (l *RecordLog) AppendBuffered(rec []byte) error {
	return l.append(rec, false)
}

func (l *RecordLog) append(rec []byte, sync bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	if l.tolerant && len(rec) == 0 {
		// Com WithUnsyncedTail, tamanho zero marca cauda zerada na abertura.
		return fmt.Errorf("%w: registro vazio", ErrInvalidConfig)
	}
	if _, err := l.file.Write(frame(nil, rec)); err != nil {
		return l.fail("gravar log", err)
	}
	if sync {
		if err := l.file.Sync(); err != nil {
			return l.fail("fsync do log", err)
		}
	}
	l.unsynced = !sync
	l.records++
	return nil
}

// Rewrite substitui todo o conteúdo do log pelos registros informados, de
// forma atômica (arquivo temporário + rename + fsync do diretório).
func (l *RecordLog) Rewrite(recs [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}

	buf := append([]byte(nil), l.header...)
	for _, r := range recs {
		buf = frame(buf, r)
	}
	tmp := l.path + ".tmp"
	if err := writeFileSync(tmp, buf); err != nil {
		return l.fail("compactar log", err)
	}
	_ = l.file.Close()
	l.file = nil
	if err := os.Rename(tmp, l.path); err != nil {
		return l.fail("substituir log", err)
	}
	if err := syncDir(filepath.Dir(l.path)); err != nil {
		return l.fail("fsync do diretorio", err)
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return l.fail("reabrir log", err)
	}
	l.file = f
	l.records = len(recs)
	l.unsynced = false
	return nil
}

// Records devolve quantos registros o arquivo contém hoje.
func (l *RecordLog) Records() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.records
}

func (l *RecordLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err == nil {
		l.err = ErrLogClosed
	}
	if l.file == nil {
		return nil
	}
	if l.unsynced {
		if err := l.file.Sync(); err != nil {
			_ = l.file.Close()
			l.file = nil
			return fmt.Errorf("storage: fsync ao fechar: %w", err)
		}
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *RecordLog) fail(op string, err error) error {
	l.err = fmt.Errorf("storage: %s: %w", op, err)
	return l.err
}

func truncateSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
