package consumer_test

import (
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/consumer"
)

func reading(sensorID string, seq uint64) client.Message {
	return client.Message{
		Topic:     "sensores/" + sensorID + "/temperatura",
		SensorID:  sensorID,
		Seq:       seq,
		Timestamp: time.Unix(100, 0),
	}
}

func TestTrackerDiscardsDuplicates(t *testing.T) {
	tr := consumer.NewTracker(0, 0)
	now := time.Unix(101, 0)

	if got := tr.Observe(reading("a", 1), now).Outcome; got != consumer.OutcomeAccepted {
		t.Fatalf("primeira entrega = %s", got)
	}
	if got := tr.Observe(reading("a", 1), now).Outcome; got != consumer.OutcomeDuplicate {
		t.Fatalf("reentrega = %s, esperado duplicata", got)
	}
	if got := tr.Observe(reading("b", 1), now).Outcome; got != consumer.OutcomeAccepted {
		t.Fatalf("mesmo seq de outro sensor = %s", got)
	}

	st := tr.Stats()
	if st.Received != 2 || st.Duplicates != 1 || st.Sensors != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTrackerDiscardsOldSeqOutsideDedupWindow(t *testing.T) {
	tr := consumer.NewTracker(1, 0)
	now := time.Unix(101, 0)

	tr.Observe(reading("a", 1), now)
	tr.Observe(reading("a", 2), now)
	// A janela de tamanho 1 já esqueceu o seq 1; a ordenação por sensor
	// ainda o reconhece como antigo (CE-16).
	if got := tr.Observe(reading("a", 1), now).Outcome; got != consumer.OutcomeOld {
		t.Fatalf("seq antigo = %s, esperado antiga", got)
	}
	if st := tr.Stats(); st.Old != 1 || st.Received != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTrackerDetectsGaps(t *testing.T) {
	tr := consumer.NewTracker(0, 0)
	now := time.Unix(101, 0)

	// O primeiro seq é a referência: sem replay, não há lacuna antes dele.
	if obs := tr.Observe(reading("a", 5), now); obs.Missing != 0 {
		t.Fatalf("primeira leitura com lacuna %d", obs.Missing)
	}

	obs := tr.Observe(reading("a", 8), now)
	if obs.Missing != 2 || obs.Marked {
		t.Fatalf("lacuna sem marca = %+v, esperado 2 faltando", obs)
	}

	gap := reading("a", 12)
	gap.Gap = true
	obs = tr.Observe(gap, now)
	if obs.Missing != 3 || !obs.Marked {
		t.Fatalf("lacuna de retencao = %+v, esperado 3 faltando com marca", obs)
	}

	st := tr.Stats()
	if st.GapsUnmarked != 1 || st.MissingUnmarked != 2 || st.GapsMarked != 1 || st.MissingMarked != 3 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTrackerCountsStatusTransitionsApart(t *testing.T) {
	tr := consumer.NewTracker(0, 0)
	now := time.Unix(101, 0)

	tr.Observe(reading("a", 1), now)
	for _, state := range []client.SensorState{client.SensorStateDormindo, client.SensorStateOffline, client.SensorStateOnline} {
		// O status tem fluxo de seq próprio e não pode ser tratado como
		// duplicata nem lacuna das leituras.
		m := client.Message{Topic: "sensores/a/status", SensorID: "a", Seq: 1, Status: state}
		if got := tr.Observe(m, now).Outcome; got != consumer.OutcomeStatus {
			t.Fatalf("status %s = %s", state, got)
		}
	}
	if obs := tr.Observe(reading("a", 2), now); obs.Outcome != consumer.OutcomeAccepted || obs.Missing != 0 {
		t.Fatalf("leitura apos status = %+v", obs)
	}

	st := tr.Stats()
	if st.StatusDormindo != 1 || st.StatusOffline != 1 || st.StatusOnline != 1 || st.Received != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestTrackerLatencyPercentiles(t *testing.T) {
	tr := consumer.NewTracker(0, 0)
	base := time.Unix(100, 0)

	for i := 1; i <= 100; i++ {
		m := reading("a", uint64(i))
		m.Timestamp = base
		tr.Observe(m, base.Add(time.Duration(i)*time.Millisecond))
	}

	st := tr.Stats()
	if st.LatencyP50 != 50*time.Millisecond || st.LatencyP95 != 95*time.Millisecond ||
		st.LatencyP99 != 99*time.Millisecond || st.LatencyMax != 100*time.Millisecond {
		t.Fatalf("latencias = p50 %s p95 %s p99 %s max %s", st.LatencyP50, st.LatencyP95, st.LatencyP99, st.LatencyMax)
	}
}
