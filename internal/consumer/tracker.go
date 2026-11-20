package consumer

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
)

// DefaultLatencySamples é o tamanho padrão da amostra de latências.
const DefaultLatencySamples = 100000

// Outcome é o destino de uma mensagem observada pelo Tracker.
type Outcome int

const (
	// OutcomeAccepted é uma leitura nova, entregue à aplicação.
	OutcomeAccepted Outcome = iota
	// OutcomeDuplicate é uma leitura repetida (mesmo sensor_id + seq) dentro
	// da janela de deduplicação (RF-14, CE-15).
	OutcomeDuplicate
	// OutcomeOld é uma leitura com seq menor ou igual ao último processado
	// daquele sensor que já saiu da janela de deduplicação (CE-16).
	OutcomeOld
	// OutcomeStatus é uma transição do tópico sensores/{id}/status (RF-21).
	OutcomeStatus
)

func (o Outcome) String() string {
	switch o {
	case OutcomeAccepted:
		return "aceita"
	case OutcomeDuplicate:
		return "duplicata"
	case OutcomeOld:
		return "antiga"
	case OutcomeStatus:
		return "status"
	default:
		return "desconhecido"
	}
}

// Observation é o resultado de observar uma mensagem.
type Observation struct {
	Outcome Outcome
	// Missing é quantos seq faltaram entre a última leitura aceita do sensor
	// e esta (RF-22). Zero quando não há lacuna.
	Missing uint64
	// Marked indica que o broker sinalizou a lacuna com GAP_BEFORE, ou seja,
	// leituras descartadas por retenção do assinante (RF-37). Lacuna sem
	// marca vem de antes do broker, tipicamente overflow do buffer do sensor
	// (RF-26, RF-31).
	Marked bool
	// Latency é agora - timestamp da leitura, que o broker calcula como
	// recebimento - age_ms (RF-19). Inclui o tempo no buffer do sensor.
	Latency time.Duration
}

// Stats resume o que o consumidor recebeu.
type Stats struct {
	// Sensors é a quantidade de sensores com ao menos uma leitura aceita.
	Sensors    int
	Received   uint64
	Duplicates uint64
	Old        uint64

	// GapsUnmarked e MissingUnmarked contam lacunas sem GAP_BEFORE e o total
	// de seq faltantes nelas.
	GapsUnmarked    uint64
	MissingUnmarked uint64
	// GapsMarked e MissingMarked contam lacunas com GAP_BEFORE (retenção).
	GapsMarked    uint64
	MissingMarked uint64

	StatusOnline   uint64
	StatusDormindo uint64
	StatusOffline  uint64

	LatencyP50 time.Duration
	LatencyP95 time.Duration
	LatencyP99 time.Duration
	LatencyMax time.Duration
}

// Tracker aplica deduplicação, ordenação por sensor e detecção de lacunas às
// mensagens entregues e acumula as métricas do consumidor. É seguro para uso
// concorrente.
//
// A política de lacunas é apenas registrar: o protocolo não tem mensagem para
// pedir recuperação (RF-22). O primeiro seq visto de um sensor é a referência,
// já que novos inscritos não recebem replay; um salto nele não é lacuna.
type Tracker struct {
	dedup *client.Dedup

	mu      sync.Mutex
	last    map[string]uint64
	stats   Stats
	latency latencySampler
}

// NewTracker cria um Tracker. Valores <= 0 usam client.DefaultDedupLimit e
// DefaultLatencySamples.
func NewTracker(dedupWindow, latencySamples int) *Tracker {
	if latencySamples <= 0 {
		latencySamples = DefaultLatencySamples
	}
	return &Tracker{
		dedup: client.NewDedup(dedupWindow),
		last:  make(map[string]uint64),
		latency: latencySampler{
			limit: latencySamples,
			rng:   rand.New(rand.NewSource(1)),
		},
	}
}

// Observe classifica a mensagem recebida em now e atualiza as métricas.
func (t *Tracker) Observe(m client.Message, now time.Time) Observation {
	if m.Status != client.SensorStateUnknown {
		t.mu.Lock()
		defer t.mu.Unlock()
		switch m.Status {
		case client.SensorStateOnline:
			t.stats.StatusOnline++
		case client.SensorStateDormindo:
			t.stats.StatusDormindo++
		case client.SensorStateOffline:
			t.stats.StatusOffline++
		}
		return Observation{Outcome: OutcomeStatus}
	}

	if !t.dedup.AcceptReading(m) {
		t.mu.Lock()
		t.stats.Duplicates++
		t.mu.Unlock()
		return Observation{Outcome: OutcomeDuplicate}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	last, seen := t.last[m.SensorID]
	if seen && m.Seq <= last {
		t.stats.Old++
		return Observation{Outcome: OutcomeOld}
	}

	obs := Observation{Outcome: OutcomeAccepted, Marked: m.Gap}
	if seen && m.Seq > last+1 {
		obs.Missing = m.Seq - last - 1
		if m.Gap {
			t.stats.GapsMarked++
			t.stats.MissingMarked += obs.Missing
		} else {
			t.stats.GapsUnmarked++
			t.stats.MissingUnmarked += obs.Missing
		}
	} else if m.Gap {
		// O broker avisou a lacuna mas não há referência para medi-la.
		t.stats.GapsMarked++
	}

	if !seen {
		t.stats.Sensors++
	}
	t.last[m.SensorID] = m.Seq
	t.stats.Received++

	obs.Latency = now.Sub(m.Timestamp)
	if obs.Latency < 0 {
		obs.Latency = 0
	}
	t.latency.add(obs.Latency)
	return obs
}

// Stats devolve um retrato das métricas acumuladas.
func (t *Tracker) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.stats
	st.LatencyP50 = t.latency.percentile(0.50)
	st.LatencyP95 = t.latency.percentile(0.95)
	st.LatencyP99 = t.latency.percentile(0.99)
	st.LatencyMax = t.latency.max
	return st
}

// latencySampler guarda uma amostra uniforme limitada (reservoir sampling)
// para obter percentis sem crescer sem limite em execuções longas.
type latencySampler struct {
	limit   int
	seen    uint64
	samples []time.Duration
	max     time.Duration
	rng     *rand.Rand
}

func (l *latencySampler) add(d time.Duration) {
	l.seen++
	if d > l.max {
		l.max = d
	}
	if len(l.samples) < l.limit {
		l.samples = append(l.samples, d)
		return
	}
	if j := l.rng.Int63n(int64(l.seen)); j < int64(l.limit) {
		l.samples[j] = d
	}
}

// percentile usa o método nearest-rank.
func (l *latencySampler) percentile(p float64) time.Duration {
	if len(l.samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(l.samples))
	copy(sorted, l.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	rank = max(0, min(rank, len(sorted)-1))
	return sorted[rank]
}
