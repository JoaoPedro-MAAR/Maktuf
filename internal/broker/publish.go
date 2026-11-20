package broker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

var (
	errInvalidTopic    = errors.New("broker: topico de publicacao invalido")
	errInvalidSensorID = errors.New("broker: sensor_id vazio")
	errInvalidStatus   = errors.New("broker: status de sensor invalido")
)

// ValidTopic aceita apenas tópicos concretos: mesmas regras de ValidFilter,
// sem curingas (RF-01).
func ValidTopic(topic string) bool {
	return ValidFilter(topic) && !strings.Contains(topic, singleLevelWildcard)
}

func validatePublish(m *protocol.Publish) error {
	if !ValidTopic(m.Topic) {
		return fmt.Errorf("%w: %q", errInvalidTopic, m.Topic)
	}
	if m.SensorID == "" {
		return errInvalidSensorID
	}
	return nil
}

// PublishResult descreve o que o broker fez com uma leitura.
type PublishResult struct {
	Status      protocol.Status
	Subscribers int
	Stale       bool
}

type sensorState struct {
	mu      sync.Mutex
	seen    bool
	lastSeq uint64
}

// publisher aplica a regra de publicação: descarta sem inscritos (RF-07),
// persiste e enfileira para todos os inscritos (RF-08, RF-50) e mantém a ordem
// por sensor descartando seq já aceito (RF-15, CE-16).
type publisher struct {
	topics     *TopicManager
	deliveries *DeliveryManager
	store      *store
	now        func() time.Time

	mu      sync.Mutex
	sensors map[string]*sensorState
}

func newPublisher(topics *TopicManager, deliveries *DeliveryManager, st *store) *publisher {
	return &publisher{
		topics:     topics,
		deliveries: deliveries,
		store:      st,
		now:        time.Now,
		sensors:    make(map[string]*sensorState),
	}
}

func (p *publisher) sensor(id string) *sensorState {
	p.mu.Lock()
	defer p.mu.Unlock()
	st, ok := p.sensors[id]
	if !ok {
		st = &sensorState{}
		p.sensors[id] = st
	}
	return st
}

// reset esquece o último seq de cada sensor (task 014).
func (p *publisher) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sensors = make(map[string]*sensorState)
}

func (p *publisher) restore(sensors map[string]uint64) {
	for sensorID, seq := range sensors {
		st := p.sensor(sensorID)
		st.seen = true
		st.lastSeq = seq
	}
}

// Publish valida, persiste e despacha a leitura. O lock por sensor garante que
// leituras do mesmo sensor entram nas filas na ordem de seq sem bloquear
// outros sensores. Se a persistência falhar, a leitura não é enfileirada e o
// erro impede o PUBACK, então o sensor mantém a leitura no buffer e reenvia.
func (p *publisher) Publish(m *protocol.Publish) (PublishResult, error) {
	if err := validatePublish(m); err != nil {
		return PublishResult{}, err
	}

	st := p.sensor(m.SensorID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.seen && m.Seq <= st.lastSeq {
		return PublishResult{Status: protocol.StatusPersisted, Stale: true}, nil
	}

	subscribers := p.topics.Subscribers(m.Topic)
	if len(subscribers) == 0 {
		return PublishResult{Status: protocol.StatusNoSubscribers}, nil
	}

	d := &protocol.Deliver{
		Topic:       m.Topic,
		SensorID:    m.SensorID,
		Seq:         m.Seq,
		TimestampMS: p.now().UnixMilli() - int64(m.AgeMS),
		Temperature: m.Temperature,
	}
	if err := p.store.publish(d, subscribers); err != nil {
		return PublishResult{}, err
	}
	for _, clientID := range subscribers {
		cp := *d
		if _, err := p.deliveries.Enqueue(clientID, &cp); err != nil {
			return PublishResult{}, err
		}
	}
	st.seen = true
	st.lastSeq = m.Seq

	return PublishResult{Status: protocol.StatusPersisted, Subscribers: len(subscribers)}, nil
}

// PublishStatus enfileira uma transição de estado gerada pelo próprio broker.
// O status usa o mesmo store e as mesmas filas do assinante, mas não passa pela
// deduplicação de leituras: o seq do status é um fluxo separado do seq do
// sensor (RF-21, RF-27 a RF-29).
func (p *publisher) PublishStatus(d *protocol.Deliver) (int, error) {
	if !ValidTopic(d.Topic) {
		return 0, fmt.Errorf("%w: %q", errInvalidTopic, d.Topic)
	}
	if d.SensorID == "" {
		return 0, errInvalidSensorID
	}
	if d.Status == protocol.SensorStateUnknown {
		return 0, errInvalidStatus
	}

	subscribers := p.topics.Subscribers(d.Topic)
	if len(subscribers) == 0 {
		return 0, nil
	}
	if err := p.store.publish(d, subscribers); err != nil {
		return 0, err
	}
	for _, clientID := range subscribers {
		cp := *d
		if _, err := p.deliveries.Enqueue(clientID, &cp); err != nil {
			return 0, err
		}
	}
	return len(subscribers), nil
}
