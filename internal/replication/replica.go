package replication

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
	"sync"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

// Target é o store da reserva visto pela replicação.
type Target interface {
	// Apply grava (com fsync) e aplica um registro do primário.
	Apply(record []byte) error
	// Reset substitui todo o estado pelos registros de uma sincronização,
	// gravando-os em disco antes de retornar.
	Reset(records [][]byte) error
}

// Replica recebe a replicação na reserva. Atende uma conexão de primário por
// vez: uma conexão nova encerra a anterior, e só começa depois que a anterior
// terminou de aplicar o que estava processando.
type Replica struct {
	ln     net.Listener
	target Target
	log    *slog.Logger

	mu       sync.Mutex
	conn     net.Conn
	lastDone chan struct{}
	wg       sync.WaitGroup
}

// Listen abre o endereço de replicação da reserva.
func Listen(addr string, target Target, log *slog.Logger) (*Replica, error) {
	if log == nil {
		log = slog.Default()
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Replica{ln: ln, target: target, log: log}, nil
}

func (r *Replica) Addr() net.Addr {
	return r.ln.Addr()
}

// Start passa a aceitar conexões do primário até Close.
func (r *Replica) Start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.serve()
	}()
}

func (r *Replica) serve() {
	r.log.Info("reserva aguardando replicacao", "replication_addr", r.ln.Addr().String())
	for {
		conn, err := r.ln.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				r.log.Error("falha ao aceitar conexao de replicacao", "error", err)
				continue
			}
			return
		}

		r.mu.Lock()
		if r.conn != nil {
			_ = r.conn.Close()
		}
		prev := r.lastDone
		done := make(chan struct{})
		r.conn, r.lastDone = conn, done
		r.mu.Unlock()

		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer close(done)
			if prev != nil {
				<-prev
			}
			r.handle(conn)
		}()
	}
}

func (r *Replica) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	log := r.log.With("primary_addr", conn.RemoteAddr().String())
	reader := bufio.NewReader(conn)

	var (
		syncing bool
		pending [][]byte
	)
	for {
		msg, err := protocol.ReadMessage(reader)
		if err != nil {
			log.Info("conexao de replicacao encerrada", "error", err)
			return
		}
		m, ok := msg.(*protocol.Replica)
		if !ok {
			log.Warn("mensagem inesperada na replicacao", "type", msg.Type().String())
			return
		}

		switch m.Kind {
		case protocol.ReplicaSyncStart:
			syncing, pending = true, nil
			continue
		case protocol.ReplicaRecord:
			if syncing {
				pending = append(pending, m.Record)
				continue
			}
		}

		status := protocol.StatusOK
		switch m.Kind {
		case protocol.ReplicaRecord:
			if err := r.target.Apply(m.Record); err != nil {
				log.Error("falha ao aplicar registro replicado", "index", m.Index, "error", err)
				status = protocol.StatusTemporaryFailure
			}
		case protocol.ReplicaSyncEnd:
			switch {
			case !syncing:
				log.Warn("SYNC_END sem SYNC_START", "index", m.Index)
				status = protocol.StatusTemporaryFailure
			default:
				if err := r.target.Reset(pending); err != nil {
					log.Error("falha ao aplicar sincronizacao", "index", m.Index, "error", err)
					status = protocol.StatusTemporaryFailure
				} else {
					log.Info("reserva sincronizada pelo primario", "records", len(pending))
				}
			}
			syncing, pending = false, nil
		}

		ack := &protocol.ReplicaAck{Status: status, Kind: m.Kind, Index: m.Index}
		if err := protocol.WriteMessage(conn, ack); err != nil {
			log.Info("falha ao enviar REPLICA_ACK", "error", err)
			return
		}
	}
}

// Close para de aceitar conexões, encerra a atual e espera a aplicação em
// andamento terminar.
func (r *Replica) Close() error {
	err := r.ln.Close()
	r.mu.Lock()
	if r.conn != nil {
		_ = r.conn.Close()
	}
	r.mu.Unlock()
	r.wg.Wait()
	return err
}
