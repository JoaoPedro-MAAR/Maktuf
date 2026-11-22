// Package storage guarda em disco o estado que precisa sobreviver a reinício
// de processo (RNF-12).
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Reading é uma leitura do sensor ainda não confirmada pelo broker.
// MeasuredAt usa o relógio local do sensor e serve apenas para obter a idade
// (age_ms) na hora do envio (DP-10).
type Reading struct {
	Seq         uint64
	MeasuredAt  time.Time
	Temperature float32
}

// Age devolve há quanto tempo a leitura foi medida, nunca negativa.
func (r Reading) Age(now time.Time) time.Duration {
	if age := now.Sub(r.MeasuredAt); age > 0 {
		return age
	}
	return 0
}

// BufferStats resume o estado do buffer. LastDroppedSeq e Dropped permitem
// detectar lacunas causadas por overflow (RF-26, RF-31).
type BufferStats struct {
	Pending        int
	LastSeq        uint64
	Dropped        uint64
	LastDroppedSeq uint64
}

// Buffer é a fila de leituras pendentes de um sensor.
type Buffer interface {
	// Append registra uma nova leitura com o próximo seq do sensor. Se o
	// buffer estiver cheio, a leitura pendente mais antiga é descartada.
	Append(measuredAt time.Time, temperature float32) (Reading, error)
	// Pending devolve as leituras pendentes em ordem crescente de seq,
	// que é a ordem de reenvio (RF-25).
	Pending() []Reading
	// Ack remove a leitura após resposta definitiva do broker (RF-24).
	Ack(seq uint64) (bool, error)
	Stats() BufferStats
	Close() error
}

var (
	ErrBufferClosed  = errors.New("storage: buffer fechado")
	ErrCorrupted     = errors.New("storage: arquivo do buffer corrompido")
	ErrInvalidConfig = errors.New("storage: configuracao invalida")
)

type BufferOptions struct {
	// MaxReadings é a quantidade máxima de leituras pendentes. Obrigatório;
	// as aplicações de exemplo usam 86.400 (requisitos.md, 11.1 item 5).
	MaxReadings int
	// SensorID identifica o sensor nos logs.
	SensorID string
	Logger   *slog.Logger
}

// Formato do arquivo (WAL append-only):
//
//	cabeçalho: magic "MKSB" + versão (uint8)
//	registros de tamanho fixo, big-endian:
//	  kind:uint8 | seq:uint64 | a:uint64 | b:uint32 | crc32:uint32
//
// O CRC (IEEE) cobre os bytes anteriores do registro. Cada operação grava no
// máximo maxWriteRecords registros de uma vez; bytes inválidos dentro dessa
// janela no fim do arquivo são uma escrita interrompida e são descartados.
// Antes dela, indicam corrupção.
const (
	walMagic   = "MKSB"
	walVersion = 1
	headerSize = len(walMagic) + 1
	recordSize = 1 + 8 + 8 + 4 + 4

	maxWriteRecords   = 2
	minCompactRecords = 64
)

type recordKind uint8

const (
	// recAppend: seq, a = MeasuredAt em ms Unix, b = bits da temperatura.
	recAppend recordKind = 1
	// recAck: seq confirmado.
	recAck recordKind = 2
	// recDrop: seq descartado por overflow.
	recDrop recordKind = 3
	// recSeq: último seq atribuído; preserva a sequência após compactação.
	recSeq recordKind = 4
	// recDropState: seq = último descartado, a = total de descartes.
	recDropState recordKind = 5
)

type record struct {
	kind recordKind
	seq  uint64
	a    uint64
	b    uint32
}

func (r record) encode(buf []byte) []byte {
	start := len(buf)
	buf = append(buf, byte(r.kind))
	buf = binary.BigEndian.AppendUint64(buf, r.seq)
	buf = binary.BigEndian.AppendUint64(buf, r.a)
	buf = binary.BigEndian.AppendUint32(buf, r.b)
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf[start:]))
}

func decodeRecord(b []byte) (record, bool) {
	body, sum := b[:recordSize-4], binary.BigEndian.Uint32(b[recordSize-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return record{}, false
	}
	r := record{
		kind: recordKind(body[0]),
		seq:  binary.BigEndian.Uint64(body[1:9]),
		a:    binary.BigEndian.Uint64(body[9:17]),
		b:    binary.BigEndian.Uint32(body[17:21]),
	}
	return r, r.kind >= recAppend && r.kind <= recDropState
}

func appendRecord(r Reading) record {
	return record{
		kind: recAppend,
		seq:  r.Seq,
		a:    uint64(r.MeasuredAt.UnixMilli()),
		b:    math.Float32bits(r.Temperature),
	}
}

// FileBuffer implementa Buffer sobre um WAL com fsync a cada operação. Pode ser
// usado por várias goroutines.
type FileBuffer struct {
	path string
	max  int
	log  *slog.Logger

	mu      sync.Mutex
	file    *os.File
	pending []Reading
	stats   BufferStats
	records int
	err     error
}

var _ Buffer = (*FileBuffer)(nil)

// OpenSensorBuffer abre (ou cria) o buffer em path e reconstrói as leituras
// pendentes a partir do arquivo.
func OpenSensorBuffer(path string, opts BufferOptions) (*FileBuffer, error) {
	if opts.MaxReadings <= 0 {
		return nil, fmt.Errorf("%w: MaxReadings deve ser positivo", ErrInvalidConfig)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	b := &FileBuffer{
		path: path,
		max:  opts.MaxReadings,
		log:  log.With("sensor_id", opts.SensorID, "buffer", path),
	}

	if err := b.load(); err != nil {
		return nil, err
	}
	for len(b.pending) > b.max {
		b.dropOldest()
	}
	// Reescrever na abertura descarta registros mortos e a cauda interrompida.
	if err := b.compact(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *FileBuffer) load() error {
	data, err := os.ReadFile(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: ler buffer: %w", err)
	}
	if len(data) < headerSize || string(data[:len(walMagic)]) != walMagic {
		return fmt.Errorf("%w: cabecalho invalido", ErrCorrupted)
	}
	if v := data[len(walMagic)]; v != walVersion {
		return fmt.Errorf("%w: versao %d nao suportada", ErrCorrupted, v)
	}

	byseq := make(map[uint64]Reading)
	body := data[headerSize:]
	for off := 0; off < len(body); off += recordSize {
		rest := len(body) - off
		var rec record
		ok := rest >= recordSize
		if ok {
			rec, ok = decodeRecord(body[off : off+recordSize])
		}
		if !ok {
			if rest > maxWriteRecords*recordSize {
				return fmt.Errorf("%w: registro invalido no byte %d", ErrCorrupted, headerSize+off)
			}
			b.log.Warn("cauda do buffer descartada (escrita interrompida)", "bytes", rest)
			break
		}
		b.apply(rec, byseq)
	}

	b.pending = b.pending[:0]
	for _, r := range byseq {
		b.pending = append(b.pending, r)
	}
	sort.Slice(b.pending, func(i, j int) bool { return b.pending[i].Seq < b.pending[j].Seq })
	return nil
}

func (b *FileBuffer) apply(rec record, byseq map[uint64]Reading) {
	switch rec.kind {
	case recAppend:
		byseq[rec.seq] = Reading{
			Seq:         rec.seq,
			MeasuredAt:  time.UnixMilli(int64(rec.a)),
			Temperature: math.Float32frombits(rec.b),
		}
	case recAck:
		delete(byseq, rec.seq)
	case recDrop:
		delete(byseq, rec.seq)
		b.stats.Dropped++
		b.stats.LastDroppedSeq = rec.seq
	case recSeq:
	case recDropState:
		b.stats.Dropped = rec.a
		b.stats.LastDroppedSeq = rec.seq
	}
	if rec.seq > b.stats.LastSeq && rec.kind != recDropState {
		b.stats.LastSeq = rec.seq
	}
}

// dropOldest remove a cabeça em memória; o registro de DROP é gravado pelo
// chamador (ou pela compactação, que persiste o estado de descartes).
func (b *FileBuffer) dropOldest() Reading {
	old := b.pending[0]
	b.pending[0] = Reading{}
	b.pending = b.pending[1:]
	b.stats.Dropped++
	b.stats.LastDroppedSeq = old.Seq
	b.log.Warn("leitura descartada por overflow do buffer",
		"seq", old.Seq, "dropped_total", b.stats.Dropped)
	return old
}

func (b *FileBuffer) Append(measuredAt time.Time, temperature float32) (Reading, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return Reading{}, b.err
	}

	r := Reading{Seq: b.stats.LastSeq + 1, MeasuredAt: measuredAt, Temperature: temperature}

	var buf []byte
	if len(b.pending) >= b.max {
		buf = record{kind: recDrop, seq: b.pending[0].Seq}.encode(buf)
	}
	buf = appendRecord(r).encode(buf)
	if err := b.writeSync(buf); err != nil {
		return Reading{}, err
	}

	if len(b.pending) >= b.max {
		b.dropOldest()
	}
	b.pending = append(b.pending, r)
	b.stats.LastSeq = r.Seq
	return r, b.maybeCompact()
}

func (b *FileBuffer) Ack(seq uint64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return false, b.err
	}

	i := sort.Search(len(b.pending), func(i int) bool { return b.pending[i].Seq >= seq })
	if i == len(b.pending) || b.pending[i].Seq != seq {
		return false, nil
	}
	if err := b.writeSync(record{kind: recAck, seq: seq}.encode(nil)); err != nil {
		return false, err
	}
	b.pending = append(b.pending[:i], b.pending[i+1:]...)
	return true, b.maybeCompact()
}

func (b *FileBuffer) Pending() []Reading {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Reading, len(b.pending))
	copy(out, b.pending)
	return out
}

func (b *FileBuffer) Stats() BufferStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stats
	s.Pending = len(b.pending)
	return s
}

func (b *FileBuffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = ErrBufferClosed
	if b.file == nil {
		return nil
	}
	err := b.file.Close()
	b.file = nil
	return err
}

// writeSync grava e força o fsync. Depois de uma falha de escrita o estado do
// arquivo é incerto, então o buffer passa a recusar operações.
func (b *FileBuffer) writeSync(buf []byte) error {
	if _, err := b.file.Write(buf); err != nil {
		b.err = fmt.Errorf("storage: gravar buffer: %w", err)
		return b.err
	}
	if err := b.file.Sync(); err != nil {
		b.err = fmt.Errorf("storage: fsync do buffer: %w", err)
		return b.err
	}
	b.records += len(buf) / recordSize
	return nil
}

func (b *FileBuffer) maybeCompact() error {
	live := len(b.pending) + 2
	if b.records-live <= max(b.max, minCompactRecords) {
		return nil
	}
	return b.compact()
}

// compact reescreve o estado vivo num arquivo temporário e o troca de forma
// atômica pelo atual (rename + fsync do diretório).
func (b *FileBuffer) compact() error {
	var buf bytes.Buffer
	buf.WriteString(walMagic)
	buf.WriteByte(walVersion)
	out := record{kind: recSeq, seq: b.stats.LastSeq}.encode(nil)
	out = record{kind: recDropState, seq: b.stats.LastDroppedSeq, a: b.stats.Dropped}.encode(out)
	for _, r := range b.pending {
		out = appendRecord(r).encode(out)
	}
	buf.Write(out)

	tmp := b.path + ".tmp"
	if err := writeFileSync(tmp, buf.Bytes()); err != nil {
		return b.fail("compactar buffer", err)
	}
	if b.file != nil {
		_ = b.file.Close()
		b.file = nil
	}
	if err := os.Rename(tmp, b.path); err != nil {
		return b.fail("substituir buffer", err)
	}
	if err := syncDir(filepath.Dir(b.path)); err != nil {
		return b.fail("fsync do diretorio", err)
	}

	f, err := os.OpenFile(b.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return b.fail("reabrir buffer", err)
	}
	b.file = f
	b.records = len(out) / recordSize
	return nil
}

func (b *FileBuffer) fail(op string, err error) error {
	b.err = fmt.Errorf("storage: %s: %w", op, err)
	return b.err
}

func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
