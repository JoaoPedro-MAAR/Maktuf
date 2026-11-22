package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration aceita durações em texto no JSON ("5s", "250ms").
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duracao deve ser texto como \"5s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// Scenario descreve um cenário de carga e falhas (task 017). Os instantes
// (falhas, janelas, start_at, burst_at) contam a partir do início dos
// sensores.
type Scenario struct {
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Seed           int64          `json:"seed"`
	Duration       Duration       `json:"duration"`
	ScrapeInterval Duration       `json:"scrape_interval"`
	Drain          Duration       `json:"drain"`
	Cluster        Cluster        `json:"cluster"`
	Sensors        []SensorGroup  `json:"sensors"`
	Consumers      []ConsumerSpec `json:"consumers"`
	Faults         []Fault        `json:"faults"`
	Goals          []Goal         `json:"goals"`
}

// Cluster descreve brokers, Juiz e vip. Sem replicated, sobe só o broker b1 e
// os clientes conectam direto nele.
type Cluster struct {
	Replicated        bool     `json:"replicated"`
	Judge             bool     `json:"judge"`
	FailureTimeout    Duration `json:"failure_timeout"`
	CheckInterval     Duration `json:"check_interval"`
	HeartbeatInterval Duration `json:"heartbeat_interval"`
	FenceTimeout      Duration `json:"fence_timeout"`
	SubscriberQueue   int      `json:"subscriber_queue"`
	SensorQueue       int      `json:"sensor_queue"`
	StatusTolerance   Duration `json:"status_tolerance"`
}

// SensorGroup vira um processo cmd/sensor. O grupo é o prefixo dos ids, que
// aparece no rótulo group das métricas do consumidor.
type SensorGroup struct {
	Group              string   `json:"group"`
	Count              int      `json:"count"`
	ContinuousRatio    float64  `json:"continuous_ratio"`
	LowInterval        Duration `json:"low_interval"`
	ContinuousInterval Duration `json:"continuous_interval"`
	Stagger            *bool    `json:"stagger"`
	BufferSize         int      `json:"buffer_size"`
	PublishTimeout     Duration `json:"publish_timeout"`
	FailureProb        float64  `json:"failure_prob"`
	FailureDowntime    Duration `json:"failure_downtime"`
	SleepProb          float64  `json:"sleep_prob"`
	SleepDuration      Duration `json:"sleep_duration"`
	LateWakeProb       float64  `json:"late_wake_prob"`
	LateWakeExtra      Duration `json:"late_wake_extra"`
	BurstAt            Duration `json:"burst_at"`
	BurstSize          int      `json:"burst_size"`
	StartAt            Duration `json:"start_at"`
}

// ConsumerSpec vira um processo cmd/consumidor.
type ConsumerSpec struct {
	ClientID     string   `json:"client_id"`
	ProcessDelay Duration `json:"process_delay"`
}

// Fault é uma falha injetada no instante At.
type Fault struct {
	At     Duration `json:"at"`
	Action string   `json:"action"`
	Target string   `json:"target"`
}

// Goal é uma meta avaliada no fim do cenário. Os campos usados dependem do
// tipo; os limites de latência e RTO estão em segundos.
type Goal struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Process  string            `json:"process"`
	Group    string            `json:"group"`
	Quantile float64           `json:"quantile"`
	Window   []Duration        `json:"window"`
	Base     []Duration        `json:"base"`
	Test     []Duration        `json:"test"`
	Fault    int               `json:"fault"`
	Metric   string            `json:"metric"`
	Labels   map[string]string `json:"labels"`
	Min      *float64          `json:"min"`
	Max      *float64          `json:"max"`
	MaxRatio float64           `json:"max_ratio"`
	// MaxExtra (s), em latency_ratio, soma uma folga absoluta ao limite:
	// teste <= max_ratio * base + max_extra.
	MaxExtra float64 `json:"max_extra"`
}

const (
	goalLatency      = "latency"
	goalLatencyRatio = "latency_ratio"
	goalThroughput   = "throughput"
	goalNoLoss       = "no_loss"
	goalRTO          = "rto"
	goalMetric       = "metric"
)

const (
	defaultScrapeInterval = time.Second
	defaultDrain          = 15 * time.Second
	defaultConsumerID     = "consumidor-1"
)

// Load lê o cenário, aplica os padrões e valida. Campos desconhecidos são
// erro, para um erro de digitação não passar despercebido.
func Load(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var s Scenario
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	s.applyDefaults()
	if err := s.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, nil
}

func (s *Scenario) applyDefaults() {
	if s.ScrapeInterval == 0 {
		s.ScrapeInterval = Duration(defaultScrapeInterval)
	}
	if s.Drain == 0 {
		s.Drain = Duration(defaultDrain)
	}
	if len(s.Consumers) == 0 {
		s.Consumers = []ConsumerSpec{{ClientID: defaultConsumerID}}
	}
}

func (s *Scenario) validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if s.Name == "" {
		add("name vazio")
	}
	if s.Duration <= 0 {
		add("duration deve ser positiva")
	}
	if s.Cluster.Judge && !s.Cluster.Replicated {
		add("cluster.judge exige cluster.replicated")
	}
	if len(s.Sensors) == 0 {
		add("nenhum grupo de sensores")
	}
	groups := map[string]bool{}
	for i, g := range s.Sensors {
		switch {
		case g.Group == "" || strings.ContainsAny(g.Group, " ,/:"):
			add("sensors[%d]: group %q invalido", i, g.Group)
		case groups[g.Group]:
			add("sensors[%d]: group %q repetido", i, g.Group)
		}
		groups[g.Group] = true
		if g.Count <= 0 {
			add("sensors[%d]: count deve ser positivo", i)
		}
		if g.StartAt.D() >= s.Duration.D() {
			add("sensors[%d]: start_at fora da duracao", i)
		}
	}
	consumers := map[string]bool{}
	for i, c := range s.Consumers {
		if c.ClientID == "" || consumers[c.ClientID] {
			add("consumers[%d]: client_id vazio ou repetido", i)
		}
		consumers[c.ClientID] = true
	}
	for i, f := range s.Faults {
		if f.At.D() < 0 || f.At.D() >= s.Duration.D() {
			add("faults[%d]: at fora da duracao", i)
		}
		if f.Action != "kill" && f.Action != "stop" && f.Action != "start" {
			add("faults[%d]: action %q (use kill, stop ou start)", i, f.Action)
		}
		if !s.validTarget(f.Target, groups, consumers) {
			add("faults[%d]: target %q invalido", i, f.Target)
		}
	}
	for i, g := range s.Goals {
		if err := s.validateGoal(g, groups, consumers); err != nil {
			add("goals[%d] (%s): %v", i, g.Name, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Scenario) validTarget(target string, groups, consumers map[string]bool) bool {
	switch {
	case target == "b1":
		return true
	case target == "primario" || target == "reserva" || target == "b2" || target == "vip":
		return s.Cluster.Replicated
	case target == "juiz":
		return s.Cluster.Judge
	case strings.HasPrefix(target, "consumidor:"):
		return consumers[strings.TrimPrefix(target, "consumidor:")]
	case strings.HasPrefix(target, "sensores:"):
		return groups[strings.TrimPrefix(target, "sensores:")]
	}
	return false
}

func (s *Scenario) validateGoal(g Goal, groups, consumers map[string]bool) error {
	if g.Name == "" {
		return errors.New("name vazio")
	}
	window := func(w []Duration) error {
		if len(w) != 2 || w[0] < 0 || w[1] <= w[0] || w[1].D() > s.Duration.D() {
			return fmt.Errorf("janela %v invalida (use [inicio, fim] dentro da duracao)", w)
		}
		return nil
	}
	consumerOK := g.Process == "" || consumers[strings.TrimPrefix(g.Process, "consumidor:")]
	switch g.Type {
	case goalLatency:
		if !groups[g.Group] || g.Quantile <= 0 || g.Quantile >= 1 || g.Max == nil || !consumerOK {
			return errors.New("latency exige group existente, quantile em (0,1), max e process de consumidor")
		}
		return window(g.Window)
	case goalLatencyRatio:
		if !groups[g.Group] || g.Quantile <= 0 || g.Quantile >= 1 || g.MaxRatio <= 0 || g.MaxExtra < 0 || !consumerOK {
			return errors.New("latency_ratio exige group, quantile, max_ratio, max_extra >= 0 e process de consumidor")
		}
		if err := window(g.Base); err != nil {
			return err
		}
		return window(g.Test)
	case goalThroughput:
		if g.Min == nil || !consumerOK {
			return errors.New("throughput exige min")
		}
		return window(g.Window)
	case goalNoLoss:
		return nil
	case goalRTO:
		if g.Fault < 0 || g.Fault >= len(s.Faults) || g.Max == nil || !consumerOK {
			return errors.New("rto exige fault (indice existente) e max")
		}
		return nil
	case goalMetric:
		if g.Process == "" || g.Metric == "" || (g.Min == nil && g.Max == nil) {
			return errors.New("metric exige process, metric e min ou max")
		}
		if g.Window != nil {
			return window(g.Window)
		}
		return nil
	}
	return fmt.Errorf("type %q desconhecido", g.Type)
}

// consumerProc devolve o processo de consumidor de uma meta (o primeiro se
// não informado).
func (s *Scenario) consumerProc(process string) string {
	if process == "" {
		return "consumidor:" + s.Consumers[0].ClientID
	}
	if strings.HasPrefix(process, "consumidor:") {
		return process
	}
	return "consumidor:" + process
}
