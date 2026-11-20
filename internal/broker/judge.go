package broker

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// errFenced impede a confirmação de uma escrita feita sem reserva e sem o
// aval do Juiz.
var errFenced = errors.New("broker: primario isolado (sem reserva e sem aval do juiz)")

const (
	judgeRetryInitial = 100 * time.Millisecond
	judgeRetryLimit   = 2 * time.Second
)

// judgeLink mantém a conexão do broker com o Juiz (task 012). Envia
// HEARTBEAT(id, estado, índice) a cada intervalo e a cada mudança, recebe o
// eco do Juiz e trata o PROMOTE. A queda do Juiz só gera log e reconexão: o
// fluxo de dados não depende dele enquanto a reserva estiver sincronizada
// (RF-48).
type judgeLink struct {
	s        *Server
	addr     string
	id       string
	interval time.Duration
	fence    time.Duration
	log      *slog.Logger

	// acked é o maior índice ecoado pelo Juiz; lastAck, o instante do último
	// eco em nanossegundos Unix.
	acked   atomic.Uint64
	lastAck atomic.Int64

	ackMu     sync.Mutex
	ackSignal chan struct{}

	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
}

func newJudgeLink(s *Server) *judgeLink {
	return &judgeLink{
		s:         s,
		addr:      s.cfg.JudgeAddr,
		id:        s.cfg.BrokerID,
		interval:  s.cfg.HeartbeatInterval,
		fence:     s.cfg.FenceTimeout,
		log:       s.log.With("broker_id", s.cfg.BrokerID, "judge_addr", s.cfg.JudgeAddr),
		ackSignal: make(chan struct{}),
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (l *judgeLink) start() {
	l.startOnce.Do(func() {
		l.started.Store(true)
		go l.run()
	})
}

func (l *judgeLink) close() {
	l.stopOnce.Do(func() {
		close(l.stop)
		if l.started.Load() {
			<-l.done
		}
	})
}

// notify pede um HEARTBEAT imediato sem bloquear.
func (l *judgeLink) notify() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// connected informa se o Juiz respondeu dentro do prazo de isolamento.
func (l *judgeLink) connected() bool {
	last := l.lastAck.Load()
	return last != 0 && time.Since(time.Unix(0, last)) <= l.fence
}

func (l *judgeLink) run() {
	defer close(l.done)
	retry := judgeRetryInitial
	for {
		conn, err := net.DialTimeout("tcp", l.addr, l.fence)
		if err != nil {
			l.log.Debug("juiz indisponivel", "error", err, "retry_in", retry)
			select {
			case <-time.After(retry):
			case <-l.stop:
				return
			}
			retry = min(2*retry, judgeRetryLimit)
			continue
		}
		retry = judgeRetryInitial
		l.log.Info("conectado ao juiz")

		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			l.readLoop(conn)
		}()
		err = l.sendLoop(conn, readDone)
		_ = conn.Close()
		<-readDone

		select {
		case <-l.stop:
			return
		default:
		}
		l.log.Warn("conexao com o juiz perdida", "error", err)
	}
}

// sendLoop envia o HEARTBEAT na conexão até ela falhar, a leitura terminar
// ou o link ser encerrado.
func (l *judgeLink) sendLoop(conn net.Conn, readDone <-chan struct{}) error {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		state, index := l.s.heartbeatState()
		hb := &protocol.Heartbeat{BrokerID: l.id, State: state, LastSeq: index}
		if err := conn.SetWriteDeadline(time.Now().Add(l.fence)); err != nil {
			return err
		}
		if err := protocol.WriteMessage(conn, hb); err != nil {
			return err
		}

		select {
		case <-ticker.C:
		case <-l.wake:
		case <-readDone:
			return errors.New("leitura do juiz encerrada")
		case <-l.stop:
			return nil
		}
	}
}

// readLoop trata o eco do HEARTBEAT e o PROMOTE.
func (l *judgeLink) readLoop(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *protocol.Heartbeat:
			l.ack(m.LastSeq)
		case *protocol.Promote:
			l.s.handlePromote(m.BrokerID)
		default:
			l.log.Warn("mensagem inesperada do juiz", "type", msg.Type().String())
			return
		}
	}
}

func (l *judgeLink) ack(index uint64) {
	l.lastAck.Store(time.Now().UnixNano())
	for {
		cur := l.acked.Load()
		if index <= cur || l.acked.CompareAndSwap(cur, index) {
			break
		}
	}
	l.ackMu.Lock()
	close(l.ackSignal)
	l.ackSignal = make(chan struct{})
	l.ackMu.Unlock()
}

func (l *judgeLink) ackChan() <-chan struct{} {
	l.ackMu.Lock()
	defer l.ackMu.Unlock()
	return l.ackSignal
}

// confirmAlone é o aval do Juiz para o primário confirmar sozinho o registro
// de índice index, com a reserva dessincronizada. Espera o Juiz ecoar um
// HEARTBEAT com índice >= index: a partir daí o Juiz sabe que o primário está
// à frente da reserva e não a promove (RPO = 0). Sem eco no prazo, o primário
// se isola. Roda sob o lock do store.
func (l *judgeLink) confirmAlone(index uint64) error {
	if l.s.fenced.Load() {
		return errFenced
	}
	timer := time.NewTimer(l.fence)
	defer timer.Stop()
	for l.acked.Load() < index {
		ch := l.ackChan()
		if l.acked.Load() >= index {
			break
		}
		l.notify()
		select {
		case <-ch:
		case <-timer.C:
			l.s.fence("reserva dessincronizada e juiz sem resposta")
			return errFenced
		case <-l.stop:
			return errFenced
		}
	}
	return nil
}

// heartbeatState devolve o estado e o índice anunciados ao Juiz.
func (s *Server) heartbeatState() (protocol.BrokerState, uint64) {
	if s.Role() == RoleBackup {
		s.haMu.Lock()
		r := s.replica
		s.haMu.Unlock()
		if r == nil {
			return protocol.BrokerStateBackupSyncing, 0
		}
		synced, applied, _ := r.Status()
		if synced {
			return protocol.BrokerStateBackupSynced, applied
		}
		return protocol.BrokerStateBackupSyncing, applied
	}
	var index uint64
	if s.primary != nil {
		index = s.primary.Index()
	}
	if s.fenced.Load() {
		return protocol.BrokerStateFenced, index
	}
	return protocol.BrokerStatePrimary, index
}

// acceptingClients informa se o broker atende clientes: só o primário não
// isolado.
func (s *Server) acceptingClients() bool {
	return s.Role() == RolePrimary && !s.fenced.Load()
}

// fence isola o primário: encerra as sessões e passa a recusar CONNECT com
// TEMPORARY_FAILURE. Os sensores mantêm as leituras no buffer.
func (s *Server) fence(reason string) {
	if s.fenced.Swap(true) {
		return
	}
	s.log.Warn("broker isolado: deixa de atender clientes", "reason", reason)
	s.closeSessions()
	if s.judge != nil {
		s.judge.notify()
	}
}

// unfence volta a atender clientes. Um primário deposto pelo Juiz só volta
// com PROMOTE(próprio id).
func (s *Server) unfence(reason string) {
	if s.deposed.Load() || !s.fenced.Swap(false) {
		return
	}
	s.log.Info("broker volta a atender clientes", "reason", reason)
	if s.judge != nil {
		s.judge.notify()
	}
}

// replicaChanged é chamado pela replicação quando a reserva sincroniza ou
// dessincroniza. Reserva sincronizada prova que ela não foi promovida, então
// o primário isolado pode voltar.
func (s *Server) replicaChanged() {
	if s.judge != nil {
		s.judge.notify()
	}
	if s.primary.Synced() {
		s.unfence("reserva sincronizada")
	}
}

// handlePromote trata o PROMOTE do Juiz: broker_id é quem deve ser o
// primário.
func (s *Server) handlePromote(id string) {
	switch {
	case id == s.cfg.BrokerID && s.Role() == RoleBackup:
		s.promote()
	case id == s.cfg.BrokerID:
		s.deposed.Store(false)
		s.unfence("juiz confirmou este primario")
	case s.Role() == RolePrimary:
		s.deposed.Store(true)
		s.fence("juiz indicou outro primario: " + id)
	default:
		s.log.Debug("PROMOTE para outro broker ignorado pela reserva", "promoted", id)
	}
}

// promote transforma a reserva em primário: para de receber a replicação,
// monta em memória o estado do store e passa a aceitar clientes. Recusa
// enquanto a conexão de replicação estiver aberta, porque o primário ainda
// está vivo (evita dois primários).
func (s *Server) promote() {
	s.haMu.Lock()
	defer s.haMu.Unlock()
	if s.Role() != RoleBackup {
		return
	}
	var index uint64
	if r := s.replica; r != nil {
		_, applied, connected := r.Status()
		if connected {
			s.log.Warn("PROMOTE recusado: o primario ainda replica nesta reserva")
			return
		}
		index = applied
		_ = r.Close()
		s.replica = nil
	}

	s.store.mu.Lock()
	// O ACK só avança o cursor; a fila é filtrada por ele aqui, como na
	// compactação feita ao abrir o store. Sem isso, leituras já confirmadas
	// pelo consumidor voltariam a ser entregues.
	s.store.state.prune()
	state := s.store.recovered()
	s.store.mu.Unlock()
	s.loadState(state)

	s.role.Store(RolePrimary)
	s.log.Info("reserva promovida a primario",
		"index", index, "subscribers", len(state.subs), "sensors", len(state.sensors))
	if s.judge != nil {
		s.judge.notify()
	}
}

// closeSessions encerra as sessões abertas sem fechar o listener.
func (s *Server) closeSessions() {
	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		_ = sess.close()
	}
}
