package juiz

import (
	"reflect"
	"testing"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const timeout = 5 * time.Second

var t0 = time.Unix(1000, 0)

// after devolve um instante já fora do período de aprendizado.
func after(d time.Duration) time.Time { return t0.Add(timeout + d) }

func view(id string, state protocol.BrokerState, index uint64, seen time.Time) brokerView {
	return brokerView{id: id, state: state, index: index, lastSeen: seen}
}

func views(vs ...brokerView) map[string]brokerView {
	out := make(map[string]brokerView, len(vs))
	for _, v := range vs {
		out[v.id] = v
	}
	return out
}

func hasEvent(res result, kind eventKind) bool {
	for _, ev := range res.events {
		if ev.kind == kind {
			return true
		}
	}
	return false
}

func expectActions(t *testing.T, res result, want ...action) {
	t.Helper()
	if len(want) == 0 && len(res.actions) == 0 {
		return
	}
	if !reflect.DeepEqual(res.actions, want) {
		t.Fatalf("acoes = %+v, esperado %+v", res.actions, want)
	}
}

const (
	primary = protocol.BrokerStatePrimary
	synced  = protocol.BrokerStateBackupSynced
	syncing = protocol.BrokerStateBackupSyncing
	fenced  = protocol.BrokerStateFenced
)

func TestNoDecisionDuringLearning(t *testing.T) {
	c := newCoordinator(timeout, t0)
	// Só a reserva está no ar, mas o Juiz acabou de subir.
	res := c.step(t0.Add(timeout/2), views(view("r", synced, 10, t0)))
	expectActions(t, res)
	if c.active != "" {
		t.Fatalf("active = %q durante o aprendizado", c.active)
	}
}

func TestFreshPrimaryIsAdoptedAndNotReplaced(t *testing.T) {
	c := newCoordinator(timeout, t0)
	now := after(0)
	res := c.step(now, views(view("p", primary, 10, now), view("r", synced, 10, now)))
	expectActions(t, res)
	if c.active != "p" || !hasEvent(res, eventAdopted) {
		t.Fatalf("active = %q, eventos %+v", c.active, res.events)
	}
}

func TestStalePrimaryPromotesSyncedBackup(t *testing.T) {
	c := newCoordinator(timeout, t0)
	c.step(after(0), views(view("p", primary, 10, after(0)), view("r", synced, 10, after(0))))

	now := after(timeout + time.Second)
	res := c.step(now, views(view("p", primary, 10, after(0)), view("r", synced, 10, now)))
	expectActions(t, res, action{to: "r", primary: "r"})
	if !hasEvent(res, eventFailoverStarted) || c.promoting != "r" {
		t.Fatalf("eventos %+v promoting %q", res.events, c.promoting)
	}

	// Enquanto a reserva não confirma, o PROMOTE é repetido a cada rodada.
	res = c.step(now.Add(time.Second), views(view("p", primary, 10, after(0)), view("r", synced, 10, now)))
	expectActions(t, res, action{to: "r", primary: "r"})

	// A reserva anuncia estado primário: failover concluído.
	done := now.Add(2 * time.Second)
	res = c.step(done, views(view("p", primary, 10, after(0)), view("r", primary, 0, done)))
	if !hasEvent(res, eventFailoverDone) || c.active != "r" {
		t.Fatalf("eventos %+v active %q", res.events, c.active)
	}
	if res.events[0].duration != 2*time.Second {
		t.Fatalf("duracao = %s, esperado 2s", res.events[0].duration)
	}
}

func TestBackupBehindOrSyncingIsNotPromoted(t *testing.T) {
	for name, backup := range map[string]brokerView{
		"atrasada":        view("r", synced, 9, time.Time{}),
		"dessincronizada": view("r", syncing, 10, time.Time{}),
		"sem heartbeat":   view("r", synced, 10, t0),
	} {
		t.Run(name, func(t *testing.T) {
			c := newCoordinator(timeout, t0)
			c.step(after(0), views(view("p", primary, 10, after(0))))
			now := after(timeout + time.Second)
			if backup.lastSeen.IsZero() {
				backup.lastSeen = now
			}
			res := c.step(now, views(view("p", primary, 10, after(0)), backup))
			expectActions(t, res)
			if !hasEvent(res, eventFailoverBlocked) {
				t.Fatalf("eventos %+v, esperado failover bloqueado", res.events)
			}
		})
	}
}

func TestOtherPrimaryIsToldWhoIsActive(t *testing.T) {
	c := newCoordinator(timeout, t0)
	now := after(0)
	c.step(now, views(view("a", primary, 10, now)))

	res := c.step(now, views(view("a", primary, 10, now), view("b", primary, 3, now)))
	expectActions(t, res, action{to: "b", primary: "a"})
	if c.shouldEcho("b", primary) || !c.shouldEcho("a", primary) || !c.shouldEcho("r", synced) {
		t.Fatalf("eco: so o primario ativo e as reservas recebem")
	}
}

func TestFencedActivePrimaryIsResumed(t *testing.T) {
	c := newCoordinator(timeout, t0)
	now := after(0)
	res := c.step(now, views(view("p", fenced, 10, now)))
	// Juiz recém-iniciado sem primário ativo: adota o isolado e o reativa.
	expectActions(t, res, action{to: "p", primary: "p"})
	if c.active != "p" {
		t.Fatalf("active = %q", c.active)
	}
}

func TestPromotionCanceledWhenPrimaryReturns(t *testing.T) {
	c := newCoordinator(timeout, t0)
	c.step(after(0), views(view("p", primary, 10, after(0)), view("r", synced, 10, after(0))))
	now := after(timeout + time.Second)
	c.step(now, views(view("p", primary, 10, after(0)), view("r", synced, 10, now)))

	// O primário volta e a reserva continua reserva (recusou o PROMOTE).
	back := now.Add(time.Second)
	res := c.step(back, views(view("p", primary, 11, back), view("r", synced, 11, back)))
	expectActions(t, res)
	if !hasEvent(res, eventFailoverCanceled) || c.promoting != "" || c.active != "p" {
		t.Fatalf("eventos %+v promoting %q active %q", res.events, c.promoting, c.active)
	}
}
