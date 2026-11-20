package juiz

import (
	"sort"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// brokerView é o que o Juiz sabe de um broker pelo último HEARTBEAT.
type brokerView struct {
	id       string
	state    protocol.BrokerState
	index    uint64
	lastSeen time.Time
}

// action pede o envio de PROMOTE(primary) ao broker to.
type action struct {
	to      string
	primary string
}

type eventKind int

const (
	// eventAdopted: o Juiz passou a considerar active o primário ativo.
	eventAdopted eventKind = iota
	// eventFailoverStarted: o primário ativo parou de enviar heartbeat.
	eventFailoverStarted
	// eventFailoverBlocked: não há reserva que possa ser promovida.
	eventFailoverBlocked
	// eventFailoverCanceled: o primário voltou antes da promoção concluir.
	eventFailoverCanceled
	// eventFailoverDone: a reserva promovida anunciou estado primário.
	eventFailoverDone
)

type event struct {
	kind     eventKind
	broker   string
	reason   string
	duration time.Duration
}

type result struct {
	actions []action
	events  []event
}

// coordinator decide promoções a partir das visões dos brokers. Não faz I/O:
// recebe o instante e as visões e devolve as mensagens a enviar e os eventos
// para log e métricas. Não é seguro para uso concorrente; o Judge o protege.
type coordinator struct {
	failureTimeout time.Duration
	startedAt      time.Time

	active        string
	promoting     string
	failoverStart time.Time
	blocked       string
}

func newCoordinator(failureTimeout time.Duration, startedAt time.Time) *coordinator {
	return &coordinator{failureTimeout: failureTimeout, startedAt: startedAt}
}

func (c *coordinator) fresh(v brokerView, now time.Time) bool {
	return now.Sub(v.lastSeen) <= c.failureTimeout
}

func isPrimaryLike(s protocol.BrokerState) bool {
	return s == protocol.BrokerStatePrimary || s == protocol.BrokerStateFenced
}

// shouldEcho diz se o Juiz responde o HEARTBEAT. Um primário que não é o
// ativo não recebe eco: sem eco, ele não consegue confirmar escritas sem
// reserva e se isola.
func (c *coordinator) shouldEcho(id string, state protocol.BrokerState) bool {
	if !isPrimaryLike(state) || c.active == "" {
		return true
	}
	return id == c.active || id == c.promoting
}

// step aplica as regras do failover (docs/protocolo.md, seção 10).
func (c *coordinator) step(now time.Time, views map[string]brokerView) result {
	var res result
	// Período de aprendizado: o Juiz recém-iniciado não decide antes de
	// ouvir os brokers por um prazo de falha completo.
	if now.Before(c.startedAt.Add(c.failureTimeout)) {
		return res
	}
	ids := sortedIDs(views)

	if c.active == "" {
		c.active = c.pick(now, views, ids, protocol.BrokerStatePrimary)
		if c.active == "" {
			c.active = c.pick(now, views, ids, protocol.BrokerStateFenced)
		}
		if c.active == "" {
			return res
		}
		res.events = append(res.events, event{kind: eventAdopted, broker: c.active})
	}

	if c.promoting != "" {
		c.checkPromotion(now, views, &res)
	}

	active := views[c.active]
	activeUp := c.fresh(active, now)

	for _, id := range ids {
		v := views[id]
		if id != c.active && id != c.promoting && c.fresh(v, now) && isPrimaryLike(v.state) {
			res.actions = append(res.actions, action{to: id, primary: c.active})
		}
	}

	if activeUp {
		c.failoverStart, c.blocked = time.Time{}, ""
		if active.state == protocol.BrokerStateFenced {
			res.actions = append(res.actions, action{to: c.active, primary: c.active})
		}
		return res
	}

	if c.failoverStart.IsZero() {
		c.failoverStart = now
		res.events = append(res.events, event{kind: eventFailoverStarted, broker: c.active})
	}
	if c.promoting != "" {
		res.actions = append(res.actions, action{to: c.promoting, primary: c.promoting})
		return res
	}

	candidate, reason := c.candidate(now, views, ids, active)
	if candidate == "" {
		if reason != c.blocked {
			c.blocked = reason
			res.events = append(res.events, event{kind: eventFailoverBlocked, broker: c.active, reason: reason})
		}
		return res
	}
	c.promoting, c.blocked = candidate, ""
	res.actions = append(res.actions, action{to: candidate, primary: candidate})
	return res
}

// checkPromotion conclui ou cancela a promoção em andamento.
func (c *coordinator) checkPromotion(now time.Time, views map[string]brokerView, res *result) {
	cand, ok := views[c.promoting]
	switch {
	case ok && c.fresh(cand, now) && cand.state == protocol.BrokerStatePrimary:
		res.events = append(res.events, event{
			kind: eventFailoverDone, broker: c.promoting, duration: now.Sub(c.failoverStart),
		})
		c.active, c.promoting = c.promoting, ""
		c.failoverStart, c.blocked = time.Time{}, ""
	case c.fresh(views[c.active], now) && isPrimaryLike(views[c.active].state):
		// A reserva não promoveu (ela recusa enquanto o primário replica) e
		// o primário voltou a falar com o Juiz.
		res.events = append(res.events, event{kind: eventFailoverCanceled, broker: c.promoting, reason: "primario voltou"})
		c.promoting = ""
	case !ok || !c.fresh(cand, now):
		res.events = append(res.events, event{kind: eventFailoverCanceled, broker: c.promoting, reason: "reserva sem heartbeat"})
		c.promoting = ""
	}
}

// candidate escolhe a reserva que pode assumir: heartbeat recente,
// sincronizada e com índice >= ao último índice do primário (nada confirmado
// pelo primário falta nela). Sem candidata, devolve o motivo.
func (c *coordinator) candidate(now time.Time, views map[string]brokerView, ids []string, active brokerView) (string, string) {
	reason := "nenhuma reserva com heartbeat recente"
	for _, id := range ids {
		v := views[id]
		if id == c.active || !c.fresh(v, now) {
			continue
		}
		switch {
		case v.state == protocol.BrokerStateBackupSynced && v.index >= active.index:
			return id, ""
		case v.state == protocol.BrokerStateBackupSynced:
			reason = "reserva atrasada em relacao ao primario"
		case v.state == protocol.BrokerStateBackupSyncing:
			reason = "reserva dessincronizada"
		}
	}
	return "", reason
}

func (c *coordinator) pick(now time.Time, views map[string]brokerView, ids []string, state protocol.BrokerState) string {
	for _, id := range ids {
		if v := views[id]; v.state == state && c.fresh(v, now) {
			return id
		}
	}
	return ""
}

func sortedIDs(views map[string]brokerView) []string {
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
