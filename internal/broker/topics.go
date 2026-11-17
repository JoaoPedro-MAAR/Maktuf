package broker

import (
	"sort"
	"strings"
	"sync"
)

const (
	topicLevelSeparator = "/"
	singleLevelWildcard = "+"
	multiLevelWildcard  = "#"
)

// TopicManager mantém em memória os tópicos ativos e seus inscritos. Um tópico
// é identificado pelo filtro de inscrição e existe apenas enquanto tiver pelo
// menos um inscrito (RF-05, DP-01). Nenhuma mensagem é armazenada aqui, então
// um novo inscrito nunca recebe publicações anteriores (RF-06, DP-03).
type TopicManager struct {
	mu     sync.RWMutex
	topics map[string]map[string]struct{}
}

func NewTopicManager() *TopicManager {
	return &TopicManager{topics: make(map[string]map[string]struct{})}
}

// Subscribe registra clientID no filtro e informa se o tópico foi criado.
// Inscrições repetidas são idempotentes.
func (m *TopicManager) Subscribe(clientID, filter string) (created bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs, ok := m.topics[filter]
	if !ok {
		subs = make(map[string]struct{})
		m.topics[filter] = subs
		created = true
	}
	subs[clientID] = struct{}{}
	return created
}

// Unsubscribe remove clientID do filtro e informa se o tópico foi removido por
// ter ficado sem inscritos. Cancelar uma inscrição inexistente não tem efeito.
func (m *TopicManager) Unsubscribe(clientID, filter string) (removed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs, ok := m.topics[filter]
	if !ok {
		return false
	}
	delete(subs, clientID)
	if len(subs) == 0 {
		delete(m.topics, filter)
		return true
	}
	return false
}

// Subscribers retorna, sem repetição e em ordem, os clientes com algum filtro
// que casa com o tópico concreto informado.
func (m *TopicManager) Subscribers(topic string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{})
	for filter, subs := range m.topics {
		if !match(filter, topic) {
			continue
		}
		for clientID := range subs {
			seen[clientID] = struct{}{}
		}
	}

	out := make([]string, 0, len(seen))
	for clientID := range seen {
		out = append(out, clientID)
	}
	sort.Strings(out)
	return out
}

func (m *TopicManager) HasTopic(filter string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.topics[filter]
	return ok
}

func (m *TopicManager) TopicCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.topics)
}

// ValidFilter rejeita filtros vazios, com nível vazio, com "+" que não ocupa um
// nível inteiro ou com "#" (curinga multinível não suportado).
func ValidFilter(filter string) bool {
	if filter == "" {
		return false
	}
	for _, level := range strings.Split(filter, topicLevelSeparator) {
		if level == "" || strings.Contains(level, multiLevelWildcard) {
			return false
		}
		if strings.Contains(level, singleLevelWildcard) && level != singleLevelWildcard {
			return false
		}
	}
	return true
}

func match(filter, topic string) bool {
	filterLevels := strings.Split(filter, topicLevelSeparator)
	topicLevels := strings.Split(topic, topicLevelSeparator)
	if len(filterLevels) != len(topicLevels) {
		return false
	}
	for i, level := range filterLevels {
		if level == singleLevelWildcard {
			if topicLevels[i] == "" {
				return false
			}
			continue
		}
		if level != topicLevels[i] {
			return false
		}
	}
	return true
}
