package broker

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	DefaultStatusTolerance    = 5 * time.Second
	DefaultStatusScanInterval = time.Second
)

// statusTopic é o tópico comum onde as transições de estado são publicadas
// (RF-21). Ele segue as mesmas regras de ciclo de vida dos tópicos de leitura.
func statusTopic(sensorID string) string {
	return "sensores/" + sensorID + "/status"
}

type sensorStatus struct {
	state    protocol.SensorState
	deadline time.Time
}

// statusManager mantém o estado de cada sensor conhecido e publica as
// transições no tópico de status. As mensagens passam pelo mesmo store e pelas
// mesmas filas dos assinantes, garantindo at-least-once e a mesma política de
// retenção das leituras.
type statusManager struct {
	mu        sync.Mutex
	states    map[string]sensorStatus
	tolerance time.Duration
	now       func() time.Time
	publish   func(*protocol.Deliver) (int, error)
	log       *slog.Logger
	seq       atomic.Uint64
}

func newStatusManager(tolerance time.Duration, publish func(*protocol.Deliver) (int, error), log *slog.Logger) *statusManager {
	if log == nil {
		log = slog.Default()
	}
	m := &statusManager{
		states:    make(map[string]sensorStatus),
		tolerance: tolerance,
		now:       time.Now,
		publish:   publish,
		log:       log,
	}
	m.seq.Store(uint64(time.Now().UnixNano()))
	return m
}

// online publica ONLINE em toda conexão válida e zera o prazo de sono
// (RF-29). O sensor é marcado como online mesmo que já estivesse online.
func (m *statusManager) online(sensorID string) {
	m.transition(sensorID, protocol.SensorStateOnline, time.Time{})
}

// sleep marca DORMINDO com prazo = agora + duração + tolerância (RF-27).
func (m *statusManager) sleep(sensorID string, duration time.Duration) {
	m.transition(sensorID, protocol.SensorStateDormindo, m.now().Add(duration).Add(m.tolerance))
}

func (m *statusManager) transition(sensorID string, state protocol.SensorState, deadline time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.states[sensorID] = sensorStatus{state: state, deadline: deadline}
	m.publishLocked(sensorID, state)
}

// expire publica OFFLINE para todo sensor dormindo cujo prazo passou sem novo
// contato (RF-28, CE-05).
func (m *statusManager) expire(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for sensorID, st := range m.states {
		if st.state != protocol.SensorStateDormindo || st.deadline.IsZero() {
			continue
		}
		if now.Before(st.deadline) {
			continue
		}
		m.states[sensorID] = sensorStatus{state: protocol.SensorStateOffline}
		m.publishLocked(sensorID, protocol.SensorStateOffline)
	}
}

// publishLocked monta a entrega de status. O seq é gerado pelo broker, separado
// do seq das leituras do mesmo sensor.
func (m *statusManager) publishLocked(sensorID string, state protocol.SensorState) {
	now := m.now()
	d := &protocol.Deliver{
		Topic:       statusTopic(sensorID),
		SensorID:    sensorID,
		Seq:         m.seq.Add(1),
		TimestampMS: now.UnixMilli(),
		Status:      state,
	}
	subscribers, err := m.publish(d)
	if err != nil {
		m.log.Warn("falha ao publicar status do sensor",
			"sensor_id", sensorID, "state", state.String(), "error", err)
		return
	}
	m.log.Debug("status do sensor publicado",
		"sensor_id", sensorID, "state", state.String(), "subscribers", subscribers)
}

// reset esquece estados e prazos de sono (task 014).
func (m *statusManager) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states = make(map[string]sensorStatus)
}

// counts devolve quantos sensores conhecidos estão em cada estado.
func (m *statusManager) counts() map[protocol.SensorState]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[protocol.SensorState]int, 3)
	for _, st := range m.states {
		out[st.state]++
	}
	return out
}

// stateOf devolve o estado atual; usado por métricas e testes.
func (m *statusManager) stateOf(sensorID string) (protocol.SensorState, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.states[sensorID]
	return st.state, st.deadline
}

func (s *session) handleSleep(log *slog.Logger, m *protocol.Sleep) error {
	if s.role != protocol.RoleSensor {
		log.Debug("SLEEP ignorado de cliente que nao e sensor")
		return nil
	}
	duration := time.Duration(m.DurationMS) * time.Millisecond
	s.server.status.sleep(s.clientID, duration)
	log.Info("sensor entrou em sono", "duration_ms", m.DurationMS)
	return nil
}
