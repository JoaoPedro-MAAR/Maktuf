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

// RecordLog é um WAL genérico com fsync a cada gravação. O conteúdo dos
// registros é opaco para o storage. Pode ser usado por várias goroutines.
type RecordLog struct {
	path    string
	header  []byte
	mu      sync.Mutex
	file    *os.File
	records int
	err     error
}

// OpenRecordLog abre (ou cria) o log em path e devolve os registros válidos,
// na ordem em que foram gravados. magic deve ter 4 bytes.
func OpenRecordLog(path, magic string, version byte) (*RecordLog, [][]byte, error) {
	if len(magic) != logMagicSize {
		return nil, nil, fmt.Errorf("%w: magic deve ter %d bytes", ErrInvalidConfig, logMagicSize)
	}
	l := &RecordLog{path: path, header: append([]byte(magic), version)}

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
		if n > len(rest)-logRecordOverhead {
			break
		}
		body := rest[logRecordOverhead:end]
		if crc32.ChecksumIEEE(body) != sum {
			if end == len(rest) {
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

// Append grava o registro e força o fsync antes de retornar. Depois de uma
// falha de escrita o estado do arquivo é incerto, então o log passa a recusar
// operações.
func (l *RecordLog) Append(rec []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return l.err
	}
	if _, err := l.file.Write(frame(nil, rec)); err != nil {
		return l.fail("gravar log", err)
	}
	if err := l.file.Sync(); err != nil {
		return l.fail("fsync do log", err)
	}
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
