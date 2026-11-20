// Package replication replica o store do broker primário na reserva (RF-40,
// RF-41, DP-04). O primário envia cada registro já gravado no próprio disco e
// espera o REPLICA_ACK da reserva antes de liberar a confirmação ao cliente.
//
// Enquanto a reserva está fora do ar, o primário segue sozinho e a reserva é
// marcada como dessincronizada; ao reconectar, o primário envia o estado
// completo antes de voltar a replicar registro a registro. As limitações dessa
// escolha estão em docs/ressalvas.md.
package replication

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	// DefaultTimeout limita a conexão com a reserva e a espera de cada
	// REPLICA_ACK.
	DefaultTimeout = 2 * time.Second

	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryLimit   = 5 * time.Second
)

var (
	errClosed   = errors.New("replication: primario encerrado")
	errBadAck   = errors.New("replication: REPLICA_ACK inesperado")
	errRejected = errors.New("replication: reserva recusou o registro")
)

// Source é o store do primário visto pela replicação.
type Source interface {
	// Snapshot chama fn com o estado vivo em registros e impede que novos
	// registros sejam gravados (e replicados) enquanto fn executa.
	Snapshot(fn func(records [][]byte) error) error
}

type PrimaryConfig struct {
	// Addr é o endereço de replicação da reserva.
	Addr string
	// Timeout limita a conexão e a espera de cada REPLICA_ACK. Valores <= 0
	// usam DefaultTimeout.
	Timeout time.Duration
	// RetryInitial é a primeira espera entre tentativas de reconexão com a
	// reserva; ela dobra a cada falha até RetryLimit.
	RetryInitial time.Duration
	RetryLimit   time.Duration
	Logger       *slog.Logger
}

// Primary mantém a conexão do primário com a reserva. A conexão só existe
// enquanto a reserva está sincronizada.
type Primary struct {
	cfg    PrimaryConfig
	log    *slog.Logger
	source Source

	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	index  uint64
	closed bool

	synced  atomic.Bool
	desyncs atomic.Uint64
	started atomic.Bool

	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func NewPrimary(cfg PrimaryConfig) *Primary {
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.RetryInitial <= 0 {
		cfg.RetryInitial = defaultRetryInitial
	}
	if cfg.RetryLimit < cfg.RetryInitial {
		cfg.RetryLimit = max(defaultRetryLimit, cfg.RetryInitial)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Primary{
		cfg:  cfg,
		log:  cfg.Logger.With("replica_addr", cfg.Addr),
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Start inicia a goroutine que conecta e sincroniza a reserva. Até a primeira
// sincronização terminar, o primário opera sozinho.
func (p *Primary) Start(src Source) {
	p.startOnce.Do(func() {
		p.source = src
		p.started.Store(true)
		go p.run()
	})
}

// Synced informa se a reserva está sincronizada, isto é, se as confirmações
// enviadas aos clientes implicam cópia na reserva.
func (p *Primary) Synced() bool {
	return p.synced.Load()
}

// Desyncs conta quantas vezes a reserva deixou de estar sincronizada.
func (p *Primary) Desyncs() uint64 {
	return p.desyncs.Load()
}

// Replicate envia um registro já gravado no primário e espera o REPLICA_ACK.
// Deve ser chamado com o lock de escrita do store, o que mantém na reserva a
// mesma ordem de registros do primário. Com a reserva dessincronizada, não
// faz nada; se a reserva falhar ou estourar o timeout, ela é marcada como
// dessincronizada e o primário segue sozinho.
func (p *Primary) Replicate(record []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil {
		return
	}
	p.index++
	msg := &protocol.Replica{Kind: protocol.ReplicaRecord, Index: p.index, Record: record}
	if err := p.roundTripLocked(msg); err != nil {
		p.desyncLocked(err)
	}
}

func (p *Primary) roundTripLocked(msg *protocol.Replica) error {
	if err := p.conn.SetDeadline(time.Now().Add(p.cfg.Timeout)); err != nil {
		return err
	}
	if err := protocol.WriteMessage(p.conn, msg); err != nil {
		return err
	}
	return p.readAckLocked(msg.Kind, msg.Index)
}

func (p *Primary) readAckLocked(kind protocol.ReplicaKind, index uint64) error {
	reply, err := protocol.ReadMessage(p.reader)
	if err != nil {
		return err
	}
	ack, ok := reply.(*protocol.ReplicaAck)
	if !ok {
		return fmt.Errorf("%w: %s", errBadAck, reply.Type())
	}
	if ack.Kind != kind || ack.Index != index {
		return fmt.Errorf("%w: %s/%d, esperado %s/%d", errBadAck, ack.Kind, ack.Index, kind, index)
	}
	if ack.Status != protocol.StatusOK {
		return fmt.Errorf("%w: status %s", errRejected, ack.Status)
	}
	return nil
}

func (p *Primary) desyncLocked(err error) {
	_ = p.conn.Close()
	p.conn = nil
	p.reader = nil
	p.synced.Store(false)
	p.desyncs.Add(1)
	p.log.Warn("reserva dessincronizada: primario segue sozinho", "error", err)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Primary) run() {
	defer close(p.done)
	retry := p.cfg.RetryInitial
	for {
		if !p.synced.Load() {
			if err := p.connect(); err != nil {
				if errors.Is(err, errClosed) {
					return
				}
				p.log.Debug("reserva indisponivel", "error", err, "retry_in", retry)
				select {
				case <-time.After(retry):
				case <-p.stop:
					return
				}
				retry = min(2*retry, p.cfg.RetryLimit)
				continue
			}
			retry = p.cfg.RetryInitial
		}
		select {
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

// connect abre a conexão e envia o estado completo. A sincronização roda
// dentro de Source.Snapshot, então nenhum registro novo é gravado até a
// reserva confirmar o SYNC_END; a partir daí cada registro é replicado.
func (p *Primary) connect() error {
	conn, err := net.DialTimeout("tcp", p.cfg.Addr, p.cfg.Timeout)
	if err != nil {
		return err
	}
	var count int
	err = p.source.Snapshot(func(records [][]byte) error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closed {
			return errClosed
		}
		count = len(records)
		p.index++
		if err := sendSync(conn, p.index, records, p.cfg.Timeout); err != nil {
			return err
		}
		p.conn = conn
		p.reader = bufio.NewReader(conn)
		if err := conn.SetReadDeadline(time.Now().Add(p.cfg.Timeout)); err != nil {
			return err
		}
		if err := p.readAckLocked(protocol.ReplicaSyncEnd, p.index); err != nil {
			p.conn, p.reader = nil, nil
			return err
		}
		p.synced.Store(true)
		return nil
	})
	if err != nil {
		_ = conn.Close()
		return err
	}
	p.log.Info("reserva sincronizada", "records", count)
	return nil
}

func sendSync(conn net.Conn, index uint64, records [][]byte, timeout time.Duration) error {
	w := bufio.NewWriter(conn)
	send := func(msg protocol.Message) error {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		return protocol.WriteMessage(w, msg)
	}
	if err := send(&protocol.Replica{Kind: protocol.ReplicaSyncStart, Index: index}); err != nil {
		return err
	}
	for _, rec := range records {
		if err := send(&protocol.Replica{Kind: protocol.ReplicaRecord, Index: index, Record: rec}); err != nil {
			return err
		}
	}
	if err := send(&protocol.Replica{Kind: protocol.ReplicaSyncEnd, Index: index}); err != nil {
		return err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	return w.Flush()
}

// Close encerra a conexão com a reserva e a goroutine de reconexão.
func (p *Primary) Close() error {
	p.closeOnce.Do(func() {
		close(p.stop)
		p.mu.Lock()
		p.closed = true
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn, p.reader = nil, nil
		}
		p.synced.Store(false)
		p.mu.Unlock()
		if p.started.Load() {
			<-p.done
		}
	})
	return nil
}
