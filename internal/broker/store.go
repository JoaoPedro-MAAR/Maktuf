package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

const (
	storeFile    = "broker.wal"
	storeMagic   = "MKBR"
	storeVersion = 1

	minCompactRecords = 1024
)

var errStoreRecord = errors.New("broker: registro do store invalido")

type storeRecordKind uint8

const (
	// recSubscribe: client_id, filter.
	recSubscribe storeRecordKind = 1
	// recUnsubscribe: client_id, filter.
	recUnsubscribe storeRecordKind = 2
	// recPublish: topic, sensor_id, seq, timestamp_ms, temperatura, lista de
	// client_id. Um único registro enfileira a leitura para todos os inscritos
	// de uma vez (RF-08, DP-05).
	recPublish storeRecordKind = 3
	// recAck: client_id, sensor_id, seq. Avança o cursor do assinante.
	recAck storeRecordKind = 4
	// recSensorSeq: sensor_id, seq. Último seq aceito do sensor; só aparece em
	// compactação, para manter o descarte de reenvios após o reinício.
	recSensorSeq storeRecordKind = 5
)

// storeState é o estado do broker que sobrevive a reinício (RF-50, RNF-12).
type storeState struct {
	subs    map[string]map[string]struct{}
	queues  map[string][]*protocol.Deliver
	cursors map[string]cursor
	sensors map[string]uint64
}

func newStoreState() *storeState {
	return &storeState{
		subs:    make(map[string]map[string]struct{}),
		queues:  make(map[string][]*protocol.Deliver),
		cursors: make(map[string]cursor),
		sensors: make(map[string]uint64),
	}
}

func (st *storeState) cursor(clientID string) cursor {
	c, ok := st.cursors[clientID]
	if !ok {
		c = make(cursor)
		st.cursors[clientID] = c
	}
	return c
}

func (st *storeState) apply(rec storeRecord) {
	switch rec.kind {
	case recSubscribe:
		set, ok := st.subs[rec.client]
		if !ok {
			set = make(map[string]struct{})
			st.subs[rec.client] = set
		}
		set[rec.filter] = struct{}{}
	case recUnsubscribe:
		if set, ok := st.subs[rec.client]; ok {
			delete(set, rec.filter)
			if len(set) == 0 {
				delete(st.subs, rec.client)
			}
		}
	case recPublish:
		d := rec.deliver
		for _, clientID := range rec.clients {
			if st.cursor(clientID).covers(d.SensorID, d.Seq) {
				continue
			}
			st.queues[clientID] = append(st.queues[clientID], d)
		}
		if d.Seq > st.sensors[d.SensorID] {
			st.sensors[d.SensorID] = d.Seq
		}
	case recAck:
		// A fila é filtrada pelo cursor só na compactação/recuperação, para
		// manter o ACK em O(1).
		st.cursor(rec.client).advance(rec.sensor, rec.seq)
	case recSensorSeq:
		if rec.seq > st.sensors[rec.sensor] {
			st.sensors[rec.sensor] = rec.seq
		}
	}
}

// prune remove das filas as leituras já confirmadas (cobertas pelo cursor).
func (st *storeState) prune() {
	for clientID, q := range st.queues {
		c := st.cursors[clientID]
		kept := q[:0]
		for _, d := range q {
			if !c.covers(d.SensorID, d.Seq) {
				kept = append(kept, d)
			}
		}
		for i := len(kept); i < len(q); i++ {
			q[i] = nil
		}
		if len(kept) == 0 {
			delete(st.queues, clientID)
			continue
		}
		st.queues[clientID] = kept
	}
}

// snapshot devolve o estado vivo como registros, em ordem determinística.
func (st *storeState) snapshot() [][]byte {
	st.prune()
	var out [][]byte
	for _, sensorID := range sortedKeys(st.sensors) {
		out = append(out, storeRecord{kind: recSensorSeq, sensor: sensorID, seq: st.sensors[sensorID]}.encode())
	}
	for _, clientID := range sortedKeys(st.subs) {
		for _, filter := range sortedKeys(st.subs[clientID]) {
			out = append(out, storeRecord{kind: recSubscribe, client: clientID, filter: filter}.encode())
		}
	}
	for _, clientID := range sortedKeys(st.cursors) {
		c := st.cursors[clientID]
		for _, sensorID := range sortedKeys(c) {
			out = append(out, storeRecord{kind: recAck, client: clientID, sensor: sensorID, seq: c[sensorID]}.encode())
		}
	}
	for _, clientID := range sortedKeys(st.queues) {
		for _, d := range st.queues[clientID] {
			out = append(out, storeRecord{kind: recPublish, deliver: d, clients: []string{clientID}}.encode())
		}
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// store persiste inscrições, leituras enfileiradas, cursores e o último seq de
// cada sensor num WAL (internal/storage). Cada operação só retorna depois do
// fsync, então o que o broker confirmou sobrevive a reinício. Mantém um espelho
// do estado vivo para compactar o log. Sem diretório, funciona apenas em
// memória e não persiste nada.
type store struct {
	mu         sync.Mutex
	log        *storage.RecordLog
	state      *storeState
	threshold  int
	minCompact int
}

// openStore abre o store em dir e devolve o estado recuperado. dir vazio
// devolve um store apenas em memória.
func openStore(dir string) (*store, *storeState, error) {
	if dir == "" {
		return &store{}, newStoreState(), nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("broker: criar diretorio de dados: %w", err)
	}
	log, recs, err := storage.OpenRecordLog(filepath.Join(dir, storeFile), storeMagic, storeVersion)
	if err != nil {
		return nil, nil, fmt.Errorf("broker: abrir store: %w", err)
	}

	st := newStoreState()
	for i, raw := range recs {
		rec, err := decodeStoreRecord(raw)
		if err != nil {
			_ = log.Close()
			return nil, nil, fmt.Errorf("broker: registro %d: %w", i, err)
		}
		st.apply(rec)
	}

	s := &store{log: log, state: st, minCompact: minCompactRecords}
	if err := s.compact(); err != nil {
		_ = log.Close()
		return nil, nil, err
	}
	return s, s.recovered(), nil
}

// recovered devolve uma cópia do estado para o servidor montar a memória.
func (s *store) recovered() *storeState {
	out := newStoreState()
	for clientID, set := range s.state.subs {
		cp := make(map[string]struct{}, len(set))
		for f := range set {
			cp[f] = struct{}{}
		}
		out.subs[clientID] = cp
	}
	for clientID, q := range s.state.queues {
		out.queues[clientID] = append([]*protocol.Deliver(nil), q...)
	}
	for clientID, c := range s.state.cursors {
		out.cursors[clientID] = c.clone()
	}
	for sensorID, seq := range s.state.sensors {
		out.sensors[sensorID] = seq
	}
	return out
}

func (s *store) persistent() bool {
	return s.log != nil
}

func (s *store) subscribe(clientID, filter string) error {
	return s.write(storeRecord{kind: recSubscribe, client: clientID, filter: filter})
}

func (s *store) unsubscribe(clientID, filter string) error {
	return s.write(storeRecord{kind: recUnsubscribe, client: clientID, filter: filter})
}

func (s *store) publish(d *protocol.Deliver, clientIDs []string) error {
	return s.write(storeRecord{kind: recPublish, deliver: d, clients: clientIDs})
}

func (s *store) ack(clientID, sensorID string, seq uint64) error {
	return s.write(storeRecord{kind: recAck, client: clientID, sensor: sensorID, seq: seq})
}

func (s *store) write(rec storeRecord) error {
	if !s.persistent() {
		return nil
	}
	raw := rec.encode()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Append(raw); err != nil {
		return fmt.Errorf("broker: persistir %s: %w", rec.kind, err)
	}
	s.state.apply(rec)
	if s.log.Records() >= s.threshold {
		return s.compact()
	}
	return nil
}

// compact reescreve o log só com o estado vivo. O próximo limite é
// proporcional ao tamanho do estado, o que deixa o custo amortizado constante.
func (s *store) compact() error {
	snap := s.state.snapshot()
	if err := s.log.Rewrite(snap); err != nil {
		return fmt.Errorf("broker: compactar store: %w", err)
	}
	s.threshold = max(2*len(snap), s.minCompact)
	return nil
}

func (s *store) close() error {
	if !s.persistent() {
		return nil
	}
	return s.log.Close()
}

type storeRecord struct {
	kind    storeRecordKind
	client  string
	filter  string
	sensor  string
	seq     uint64
	deliver *protocol.Deliver
	clients []string
}

func (k storeRecordKind) String() string {
	switch k {
	case recSubscribe:
		return "SUBSCRIBE"
	case recUnsubscribe:
		return "UNSUBSCRIBE"
	case recPublish:
		return "PUBLISH"
	case recAck:
		return "DELIVER_ACK"
	case recSensorSeq:
		return "SENSOR_SEQ"
	default:
		return fmt.Sprintf("registro(%d)", uint8(k))
	}
}

// Codificação dos registros: kind:uint8 seguido dos campos em big-endian;
// strings com prefixo de tamanho uint16, como no protocolo.
func (r storeRecord) encode() []byte {
	buf := []byte{byte(r.kind)}
	switch r.kind {
	case recSubscribe, recUnsubscribe:
		buf = appendString(buf, r.client)
		buf = appendString(buf, r.filter)
	case recPublish:
		d := r.deliver
		buf = appendString(buf, d.Topic)
		buf = appendString(buf, d.SensorID)
		buf = binary.BigEndian.AppendUint64(buf, d.Seq)
		buf = binary.BigEndian.AppendUint64(buf, uint64(d.TimestampMS))
		buf = binary.BigEndian.AppendUint32(buf, math.Float32bits(d.Temperature))
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(r.clients)))
		for _, c := range r.clients {
			buf = appendString(buf, c)
		}
	case recAck:
		buf = appendString(buf, r.client)
		buf = appendString(buf, r.sensor)
		buf = binary.BigEndian.AppendUint64(buf, r.seq)
	case recSensorSeq:
		buf = appendString(buf, r.sensor)
		buf = binary.BigEndian.AppendUint64(buf, r.seq)
	}
	return buf
}

func appendString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s)))
	return append(buf, s...)
}

type recordReader struct {
	b   []byte
	err error
}

func (r *recordReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < n {
		r.err = errStoreRecord
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *recordReader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (r *recordReader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (r *recordReader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

func (r *recordReader) str() string {
	return string(r.take(int(r.u16())))
}

func decodeStoreRecord(raw []byte) (storeRecord, error) {
	r := &recordReader{b: raw}
	var rec storeRecord
	if b := r.take(1); b != nil {
		rec.kind = storeRecordKind(b[0])
	}
	switch rec.kind {
	case recSubscribe, recUnsubscribe:
		rec.client = r.str()
		rec.filter = r.str()
	case recPublish:
		d := &protocol.Deliver{}
		d.Topic = r.str()
		d.SensorID = r.str()
		d.Seq = r.u64()
		d.TimestampMS = int64(r.u64())
		d.Temperature = math.Float32frombits(r.u32())
		n := int(r.u16())
		for i := 0; i < n && r.err == nil; i++ {
			rec.clients = append(rec.clients, r.str())
		}
		rec.deliver = d
	case recAck:
		rec.client = r.str()
		rec.sensor = r.str()
		rec.seq = r.u64()
	case recSensorSeq:
		rec.sensor = r.str()
		rec.seq = r.u64()
	default:
		return storeRecord{}, fmt.Errorf("%w: tipo %d", errStoreRecord, rec.kind)
	}
	if r.err != nil {
		return storeRecord{}, r.err
	}
	if len(r.b) != 0 {
		return storeRecord{}, fmt.Errorf("%w: bytes sobrando", errStoreRecord)
	}
	return rec, nil
}
