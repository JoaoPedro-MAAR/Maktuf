package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

var (
	errInvalidTopic    = errors.New("broker: topico de publicacao invalido")
	errInvalidSensorID = errors.New("broker: sensor_id vazio")
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
		p.deliveries.Enqueue(clientID, &cp)
	}
	st.seen = true
	st.lastSeq = m.Seq

	return PublishResult{Status: protocol.StatusPersisted, Subscribers: len(subscribers)}, nil
}

func (s *session) handlePublish(log *slog.Logger, m *protocol.Publish) error {
	res, err := s.server.publisher.Publish(m)
	if err != nil {
		return err
	}
	if err := s.write(&protocol.PubAck{Status: res.Status, Seq: m.Seq}); err != nil {
		return err
	}
	log.Debug("leitura recebida",
		"topic", m.Topic, "sensor_id", m.SensorID, "seq", m.Seq,
		"status", res.Status.String(), "subscribers", res.Subscribers, "stale", res.Stale)
	return nil
}
