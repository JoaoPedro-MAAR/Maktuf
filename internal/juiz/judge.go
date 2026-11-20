// Package juiz implementa o Juiz do Maktuf (task 012, DP-12): recebe o
// HEARTBEAT dos brokers, detecta a falha do primário e promove a reserva
// sincronizada com PROMOTE. O Juiz não recebe leituras e não está no caminho
// dos dados; se ele cair, os brokers seguem atendendo e o failover só volta a
// funcionar quando ele voltar (RF-48). As regras estão em docs/protocolo.md,
// seção 10.
package juiz

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	DefaultAddr           = ":7100"
	DefaultFailureTimeout = 5 * time.Second
	DefaultCheckInterval  = 500 * time.Millisecond

	writeTimeout = time.Second
)

var (
	ErrInvalidConfig = errors.New("juiz: configuracao invalida")
	errNotListening  = errors.New("juiz: nao esta escutando")
)

// Config configura o Juiz. Campos zerados recebem os defaults.
type Config struct {
	Addr string
	// FailureTimeout é quanto tempo sem HEARTBEAT faz um broker ser
	// considerado fora do ar. Também é o período de aprendizado inicial.
	FailureTimeout time.Duration
	// CheckInterval é o período da avaliação de failover.
	CheckInterval time.Duration
	// Now é o relógio; nil usa time.Now.
	Now     func() time.Time
	Metrics *metrics.Registry
	Logger  *slog.Logger
}

// brokerConn serializa as escritas numa conexão de broker: o eco sai da
// goroutine de leitura e o PROMOTE, da avaliação.
type brokerConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func (b *brokerConn) send(msg protocol.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return protocol.WriteMessage(b.conn, msg)
}

// Judge é o coordenador de failover.
type Judge struct {
	cfg     Config
	log     *slog.Logger
	ln      net.Listener
	metrics *judgeMetrics

	mu    sync.Mutex
	views map[string]brokerView
	conns map[string]*brokerConn
	open  map[net.Conn]struct{}
	coord *coordinator

	wg sync.WaitGroup
}

// New valida a configuração e prepara o Juiz.
func New(cfg Config) (*Judge, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.FailureTimeout <= 0 {
		cfg.FailureTimeout = DefaultFailureTimeout
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = DefaultCheckInterval
	}
	if cfg.CheckInterval > cfg.FailureTimeout {
		return nil, fmt.Errorf("%w: check-interval maior que failure-timeout", ErrInvalidConfig)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	j := &Judge{
		cfg:   cfg,
		log:   cfg.Logger,
		views: make(map[string]brokerView),
		conns: make(map[string]*brokerConn),
		open:  make(map[net.Conn]struct{}),
		coord: newCoordinator(cfg.FailureTimeout, cfg.Now()),
	}
	j.metrics = newJudgeMetrics(cfg.Metrics, j)
	return j, nil
}

// Metrics devolve o registro com as métricas do Juiz.
func (j *Judge) Metrics() *metrics.Registry { return j.cfg.Metrics }

// Listen abre o endereço do Juiz.
func (j *Judge) Listen() error {
	ln, err := net.Listen("tcp", j.cfg.Addr)
	if err != nil {
		return err
	}
	j.ln = ln
	return nil
}

// Addr devolve o endereço em que o Juiz escuta; nil antes de Listen.
func (j *Judge) Addr() net.Addr {
	if j.ln == nil {
		return nil
	}
	return j.ln.Addr()
}

// Serve atende os brokers e avalia o failover até ctx terminar, o que
// encerra normalmente e devolve nil.
func (j *Judge) Serve(ctx context.Context) error {
	if j.ln == nil {
		return errNotListening
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		j.evaluateLoop(ctx)
	}()
	go func() {
		<-ctx.Done()
		_ = j.ln.Close()
		j.mu.Lock()
		for conn := range j.open {
			_ = conn.Close()
		}
		j.mu.Unlock()
	}()

	j.log.Info("juiz escutando", "addr", j.ln.Addr().String(),
		"failure_timeout", j.cfg.FailureTimeout, "check_interval", j.cfg.CheckInterval)
	for {
		conn, err := j.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				cancel()
				j.wg.Wait()
				return nil
			}
			j.log.Error("falha ao aceitar conexao", "error", err)
			continue
		}
		j.mu.Lock()
		if ctx.Err() != nil {
			// O encerramento já fechou as conexões abertas.
			j.mu.Unlock()
			_ = conn.Close()
			continue
		}
		j.open[conn] = struct{}{}
		j.mu.Unlock()
		j.wg.Add(1)
		go func() {
			defer j.wg.Done()
			j.handleConn(conn)
		}()
	}
}

// ListenAndServe é Listen seguido de Serve.
func (j *Judge) ListenAndServe(ctx context.Context) error {
	if err := j.Listen(); err != nil {
		return err
	}
	return j.Serve(ctx)
}

// handleConn lê os HEARTBEATs de um broker, atualiza a visão dele e responde
// com o eco.
func (j *Judge) handleConn(conn net.Conn) {
	bc := &brokerConn{conn: conn}
	log := j.log.With("remote_addr", conn.RemoteAddr().String())
	var id string
	defer func() {
		_ = conn.Close()
		j.mu.Lock()
		delete(j.open, conn)
		if id != "" && j.conns[id] == bc {
			delete(j.conns, id)
		}
		j.mu.Unlock()
		if id != "" {
			log.Info("conexao do broker encerrada", "broker_id", id)
		}
	}()

	r := bufio.NewReader(conn)
	for {
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			return
		}
		hb, ok := msg.(*protocol.Heartbeat)
		if !ok || hb.BrokerID == "" || hb.BrokerID == protocol.JudgeID || (id != "" && hb.BrokerID != id) {
			log.Warn("mensagem invalida do broker; conexao encerrada", "type", msg.Type().String())
			return
		}
		id = hb.BrokerID

		j.mu.Lock()
		prev, known := j.views[id]
		j.views[id] = brokerView{id: id, state: hb.State, index: hb.LastSeq, lastSeen: j.cfg.Now()}
		if old := j.conns[id]; old != nil && old != bc {
			_ = old.conn.Close()
		}
		j.conns[id] = bc
		echo := j.coord.shouldEcho(id, hb.State)
		j.mu.Unlock()

		if !known || prev.state != hb.State {
			log.Info("estado do broker", "broker_id", id, "state", hb.State.String(), "last_seq", hb.LastSeq)
		}
		if echo {
			reply := &protocol.Heartbeat{BrokerID: protocol.JudgeID, LastSeq: hb.LastSeq}
			if err := bc.send(reply); err != nil {
				return
			}
		}
	}
}

func (j *Judge) evaluateLoop(ctx context.Context) {
	ticker := time.NewTicker(j.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.evaluate()
		}
	}
}

// evaluate roda uma rodada de decisão e envia os PROMOTE resultantes.
func (j *Judge) evaluate() {
	now := j.cfg.Now()
	j.mu.Lock()
	views := make(map[string]brokerView, len(j.views))
	for id, v := range j.views {
		views[id] = v
	}
	res := j.coord.step(now, views)
	targets := make([]*brokerConn, len(res.actions))
	for i, a := range res.actions {
		targets[i] = j.conns[a.to]
	}
	j.mu.Unlock()

	for _, ev := range res.events {
		j.logEvent(ev)
	}
	for i, a := range res.actions {
		log := j.log.With("to", a.to, "primary", a.primary)
		if targets[i] == nil {
			log.Warn("PROMOTE nao enviado: broker sem conexao")
			continue
		}
		if err := targets[i].send(&protocol.Promote{BrokerID: a.primary}); err != nil {
			log.Warn("falha ao enviar PROMOTE", "error", err)
			continue
		}
		if a.to == a.primary {
			j.metrics.promotions.Inc(a.to)
			log.Info("PROMOTE enviado")
		} else {
			log.Warn("PROMOTE enviado a broker que nao e o primario ativo")
		}
	}
}

func (j *Judge) logEvent(ev event) {
	switch ev.kind {
	case eventAdopted:
		j.log.Info("primario ativo reconhecido", "broker_id", ev.broker)
	case eventFailoverStarted:
		j.log.Warn("primario sem heartbeat: avaliando failover", "broker_id", ev.broker)
	case eventFailoverBlocked:
		j.log.Warn("failover impossivel", "primary", ev.broker, "reason", ev.reason)
	case eventFailoverCanceled:
		j.log.Warn("promocao cancelada", "broker_id", ev.broker, "reason", ev.reason)
	case eventFailoverDone:
		j.metrics.failover.ObserveDuration(ev.duration)
		j.log.Info("failover concluido", "primary", ev.broker, "duration", ev.duration)
	}
}

// snapshot devolve as visões e o primário ativo, para as métricas.
func (j *Judge) snapshot() (map[string]brokerView, time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	views := make(map[string]brokerView, len(j.views))
	for id, v := range j.views {
		views[id] = v
	}
	return views, j.cfg.Now()
}
