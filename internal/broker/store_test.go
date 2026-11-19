package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

func mustOpenStore(t *testing.T, dir string) (*store, *storeState) {
	t.Helper()
	s, st, err := openStore(dir)
	if err != nil {
		t.Fatalf("openStore: %v", err)
	}
	t.Cleanup(func() { _ = s.close() })
	return s, st
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func seqs(q []*protocol.Deliver) []string {
	out := make([]string, len(q))
	for i, d := range q {
		out[i] = fmt.Sprintf("%s/%d", d.SensorID, d.Seq)
	}
	return out
}

func queueSeqs(q *readingQueue) []string {
	if q == nil {
		return nil
	}
	return seqs(q.items)
}

func TestStoreRecoversState(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)

	must(t, s.subscribe("c1", "sensores/+/temperatura"))
	must(t, s.subscribe("c2", "sensores/a/temperatura"))
	must(t, s.subscribe("c2", "sensores/b/temperatura"))
	must(t, s.unsubscribe("c2", "sensores/b/temperatura"))
	must(t, s.publish(deliver("a", 1), []string{"c1", "c2"}))
	must(t, s.publish(deliver("b", 1), []string{"c1"}))
	must(t, s.publish(deliver("a", 2), []string{"c1", "c2"}))
	must(t, s.ack("c1", "a", 1))
	must(t, s.ack("c2", "a", 1))
	must(t, s.ack("c2", "a", 2))
	must(t, s.close())

	_, st := mustOpenStore(t, dir)

	wantSubs := map[string]map[string]struct{}{
		"c1": {"sensores/+/temperatura": {}},
		"c2": {"sensores/a/temperatura": {}},
	}
	if !reflect.DeepEqual(st.subs, wantSubs) {
		t.Fatalf("subs = %v, esperado %v", st.subs, wantSubs)
	}
	if got := queueSeqs(st.queues["c1"]); !reflect.DeepEqual(got, []string{"b/1", "a/2"}) {
		t.Fatalf("fila c1 = %v", got)
	}
	if _, ok := st.queues["c2"]; ok {
		t.Fatalf("fila c2 deveria estar vazia: %v", queueSeqs(st.queues["c2"]))
	}
	if st.cursors["c1"]["a"] != 1 || st.cursors["c2"]["a"] != 2 {
		t.Fatalf("cursores = %v", st.cursors)
	}
	if st.sensors["a"] != 2 || st.sensors["b"] != 1 {
		t.Fatalf("sensores = %v", st.sensors)
	}
}

func TestStoreRecoversPublishFields(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	d := &protocol.Deliver{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 9, TimestampMS: -42, Temperature: 21.5}
	must(t, s.publish(d, []string{"c1"}))
	must(t, s.close())

	_, st := mustOpenStore(t, dir)
	q := st.queues["c1"]
	if q == nil || len(q.items) != 1 || !reflect.DeepEqual(q.items[0], d) {
		t.Fatalf("leitura recuperada = %#v, esperado %#v", st.queues["c1"], d)
	}
}

// Cada operação faz fsync: reabrir sem fechar (como após morte abrupta do
// processo) recupera tudo o que já foi confirmado.
func TestStoreSurvivesWithoutClose(t *testing.T) {
	dir := t.TempDir()
	s, _, err := openStore(dir)
	must(t, err)
	must(t, s.subscribe("c1", "sensores/a/temperatura"))
	must(t, s.publish(deliver("a", 1), []string{"c1"}))

	_, st := mustOpenStore(t, dir)
	if st.queues["c1"].len() != 1 || len(st.subs["c1"]) != 1 {
		t.Fatalf("estado recuperado incompleto: subs=%v fila=%v", st.subs, st.queues)
	}
	_ = s.close()
}

func TestStoreCompactionKeepsStateAndShrinksLog(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	s.minCompact, s.threshold = 16, 16
	must(t, s.subscribe("c1", "sensores/a/temperatura"))

	const total = 3 * 16
	for seq := uint64(1); seq <= total; seq++ {
		must(t, s.publish(deliver("a", seq), []string{"c1"}))
		if seq < total {
			must(t, s.ack("c1", "a", seq))
		}
	}
	if n := s.log.Records(); n >= 16 {
		t.Fatalf("log com %d registros; esperava compactacao", n)
	}
	must(t, s.close())

	_, st := mustOpenStore(t, dir)
	if got := st.queues["c1"].items; len(got) != 1 || got[0].Seq != total {
		t.Fatalf("fila = %v, esperado so a/%d", seqs(got), total)
	}
	if st.cursors["c1"]["a"] != total-1 || st.sensors["a"] != total {
		t.Fatalf("cursor=%v sensores=%v", st.cursors, st.sensors)
	}
}

func TestStoreMemoryOnlyPersistsNothing(t *testing.T) {
	s, st := mustOpenStore(t, "")
	if s.persistent() {
		t.Fatal("store sem diretorio nao deveria gravar em disco")
	}
	must(t, s.subscribe("c1", "x"))
	must(t, s.publish(deliver("a", 1), []string{"c1"}))
	if len(st.subs) != 0 || len(st.queues) != 0 {
		t.Fatal("store em memoria nao deve guardar estado")
	}
}

func TestStoreRejectsCorruptedRecord(t *testing.T) {
	dir := t.TempDir()
	l, _, err := storage.OpenRecordLog(filepath.Join(dir, storeFile), storeMagic, storeVersion)
	must(t, err)
	must(t, l.Append([]byte{99}))
	must(t, l.Close())

	if _, _, err := openStore(dir); !errors.Is(err, errStoreRecord) {
		t.Fatalf("erro = %v, esperado %v", err, errStoreRecord)
	}
}

func TestStoreDropAndGapSurviveRestartAndCompaction(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	must(t, s.publish(deliver("a", 1), []string{"c1"}))
	must(t, s.publish(deliver("a", 2), []string{"c1"}))
	must(t, s.publish(deliver("a", 3), []string{"c1"}))
	must(t, s.drop("c1", "a", 2))
	must(t, s.compact())
	must(t, s.close())

	_, st := mustOpenStore(t, dir)
	q := st.queues["c1"]
	if q == nil {
		t.Fatal("fila nao foi recuperada")
	}
	if got := seqs(q.items); !reflect.DeepEqual(got, []string{"a/1", "a/3"}) {
		t.Fatalf("fila = %v", got)
	}
	if q.items[1].Flags&protocol.DeliverFlagGap == 0 {
		t.Fatal("a leitura 3 deveria carregar a lacuna apos o reinicio")
	}
}

func TestStoreStatusPersistsInSeparateStream(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	must(t, s.publish(deliver("a", 5), []string{"c1"}))
	status := &protocol.Deliver{
		Topic: statusTopic("a"), SensorID: "a", Seq: 99,
		Status: protocol.SensorStateDormindo,
	}
	must(t, s.publish(status, []string{"c1"}))
	must(t, s.close())

	_, st := mustOpenStore(t, dir)
	q := st.queues["c1"]
	if q == nil || len(q.items) != 2 {
		t.Fatalf("fila recuperada = %#v", q)
	}
	if q.items[1].Status != protocol.SensorStateDormindo {
		t.Fatalf("status recuperado = %s", q.items[1].Status)
	}
	if st.sensors["a"] != 5 {
		t.Fatalf("seq de status avancou o sensor de leitura: %v", st.sensors)
	}
	if st.cursors["c1"]["a"] != 0 {
		t.Fatalf("cursor de leitura nao deveria existir: %v", st.cursors)
	}
}

func TestStorePendingGapSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	must(t, s.publish(deliver("a", 1), []string{"c1"}))
	must(t, s.drop("c1", "a", 1))
	must(t, s.close())

	_, st := mustOpenStore(t, dir)
	q := st.queues["c1"]
	if q == nil || !q.gaps["a"] {
		t.Fatalf("lacuna pendente nao recuperada: %#v", q)
	}
}

// Sem persistência, a leitura não é enfileirada e Publish devolve erro, o que
// impede o PUBACK (RF-13).
func TestPublishFailsWhenStoreFails(t *testing.T) {
	dir := t.TempDir()
	s, _ := mustOpenStore(t, dir)
	topics := NewTopicManager()
	topics.Subscribe("c1", "sensores/a/temperatura")
	deliveries := newDeliveryManager(s, nil, 0)
	p := newPublisher(topics, deliveries, s)
	must(t, s.close())

	if _, err := p.Publish(&protocol.Publish{Topic: "sensores/a/temperatura", SensorID: "a", Seq: 1}); err == nil {
		t.Fatal("Publish deveria falhar com o store fechado")
	}
	if n := deliveries.Pending("c1"); n != 0 {
		t.Fatalf("pendentes = %d, esperado 0", n)
	}
}
