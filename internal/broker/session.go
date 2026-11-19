package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

var (
	errInvalidFilter = errors.New("broker: filtro de topico invalido")
	errSessionClosed = errors.New("broker: sessao encerrada")
)

type session struct {
	server    *Server
	conn      net.Conn
	reader    *bufio.Reader
	clientID  string
	role      protocol.Role
	writeMu   sync.Mutex
	log       *slog.Logger
	queue     *sensorQueue
	acks      chan *publishJob
	closed    chan struct{}
	closeOnce sync.Once
}

func newSession(s *Server, conn net.Conn) *session {
	return &session{
		server: s,
		conn:   conn,
		reader: bufio.NewReader(conn),
		closed: make(chan struct{}),
	}
}

func (s *session) serve(ctx context.Context) {
	defer func() { _ = s.close() }()

	log := s.server.log.With("remote_addr", s.conn.RemoteAddr().String())

	msg, err := protocol.ReadMessage(s.reader)
	if err != nil {
		log.Warn("falha ao ler CONNECT", "error", err)
		return
	}

	connect, ok := msg.(*protocol.Connect)
	if !ok {
		log.Warn("primeira mensagem nao e CONNECT", "type", msg.Type().String())
		return
	}

	log = log.With("client_id", connect.ClientID, "role", connect.Role.String())

	if connect.ClientID == "" || !validRole(connect.Role) {
		log.Warn("CONNECT invalido")
		return
	}

	if err := s.write(&protocol.ConnAck{Status: protocol.StatusOK}); err != nil {
		log.Warn("falha ao enviar CONNACK", "error", err)
		return
	}

	s.clientID = connect.ClientID
	s.role = connect.Role
	s.log = log
	log.Info("cliente conectado")

	if connect.Role == protocol.RoleSensor {
		s.server.status.online(s.clientID)
	}

	deliveries := s.server.deliveries
	deliveries.attach(s.clientID, s)
	defer deliveries.detach(s.clientID, s)

	limit := s.server.cfg.SensorQueueLimit
	q := &sensorQueue{clientID: s.clientID, slots: make(chan struct{}, limit)}
	s.queue = q
	s.acks = make(chan *publishJob, limit)
	s.server.scheduler.register(q)
	defer func() {
		_ = s.close()
		s.server.scheduler.remove(q)
	}()

	s.server.wg.Add(1)
	go func() {
		defer s.server.wg.Done()
		s.ackLoop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := protocol.ReadMessage(s.reader)
		if err != nil {
			log.Info("conexao encerrada", "error", err)
			return
		}

		switch m := msg.(type) {
		case *protocol.Subscribe:
			err = s.handleSubscribe(log, m)
		case *protocol.Unsubscribe:
			err = s.handleUnsubscribe(log, m)
		case *protocol.Publish:
			err = s.handlePublish(log, m)
		case *protocol.DeliverAck:
			err = s.handleDeliverAck(log, m)
		case *protocol.Sleep:
			err = s.handleSleep(log, m)
		default:
			log.Debug("mensagem recebida (nao tratada)", "type", msg.Type().String())
		}
		if err != nil {
			log.Warn("encerrando conexao", "type", msg.Type().String(), "error", err)
			return
		}
	}
}

func (s *session) handleSubscribe(log *slog.Logger, m *protocol.Subscribe) error {
	if !ValidFilter(m.TopicFilter) {
		return fmt.Errorf("%w: %q", errInvalidFilter, m.TopicFilter)
	}
	if err := s.server.store.subscribe(s.clientID, m.TopicFilter); err != nil {
		return err
	}
	created := s.server.topics.Subscribe(s.clientID, m.TopicFilter)
	if err := s.write(&protocol.SubAck{Status: protocol.StatusOK}); err != nil {
		return err
	}
	log.Info("inscricao registrada", "topic_filter", m.TopicFilter, "topic_created", created)
	return nil
}

func (s *session) handleUnsubscribe(log *slog.Logger, m *protocol.Unsubscribe) error {
	if !ValidFilter(m.TopicFilter) {
		return fmt.Errorf("%w: %q", errInvalidFilter, m.TopicFilter)
	}
	if err := s.server.store.unsubscribe(s.clientID, m.TopicFilter); err != nil {
		return err
	}
	removed := s.server.topics.Unsubscribe(s.clientID, m.TopicFilter)
	if err := s.write(&protocol.UnsubAck{Status: protocol.StatusOK}); err != nil {
		return err
	}
	log.Info("inscricao removida", "topic_filter", m.TopicFilter, "topic_removed", removed)
	return nil
}

func (s *session) write(msg protocol.Message) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return protocol.WriteMessage(s.conn, msg)
}

func (s *session) deliveryFailed(err error) {
	s.log.Warn("falha ao enviar DELIVER", "error", err)
	_ = s.conn.Close()
}

func (s *session) close() error {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.conn.Close()
	})
	return nil
}

// handlePublish valida e entrega a leitura ao scheduler. A sessão só bloqueia
// quando a própria fila enche: nesse caso para de ler o socket até o ackLoop
// liberar um slot com o PUBACK, aplicando backpressure apenas a este sensor.
func (s *session) handlePublish(_ *slog.Logger, m *protocol.Publish) error {
	if err := validatePublish(m); err != nil {
		return err
	}

	select {
	case <-s.closed:
		return errSessionClosed
	default:
	}

	select {
	case s.queue.slots <- struct{}{}:
	default:
		s.server.scheduler.waits.Add(1)
		select {
		case s.queue.slots <- struct{}{}:
		case <-s.closed:
			return errSessionClosed
		}
	}

	job := &publishJob{msg: m, done: make(chan publishOutcome, 1)}
	select {
	case s.acks <- job:
	case <-s.closed:
		return errSessionClosed
	}
	s.server.scheduler.enqueue(s.queue, job)
	return nil
}

// ackLoop responde os PUBLISH na ordem em que foram aceitos. O slot só é
// liberado depois que o PUBACK é escrito: um sensor que não lê as respostas
// também fica limitado sozinho.
func (s *session) ackLoop() {
	for {
		var job *publishJob
		select {
		case job = <-s.acks:
		case <-s.closed:
			return
		}

		var outcome publishOutcome
		select {
		case outcome = <-job.done:
		case <-s.closed:
			return
		}

		if outcome.err != nil {
			s.log.Warn("falha ao processar leitura",
				"topic", job.msg.Topic, "sensor_id", job.msg.SensorID, "seq", job.msg.Seq,
				"error", outcome.err)
			_ = s.close()
			return
		}

		if err := s.write(&protocol.PubAck{Status: outcome.res.Status, Seq: job.msg.Seq}); err != nil {
			s.log.Warn("falha ao enviar PUBACK", "error", err)
			_ = s.close()
			return
		}
		<-s.queue.slots

		s.log.Debug("leitura recebida",
			"topic", job.msg.Topic, "sensor_id", job.msg.SensorID, "seq", job.msg.Seq,
			"status", outcome.res.Status.String(), "subscribers", outcome.res.Subscribers, "stale", outcome.res.Stale)
	}
}

func validRole(role protocol.Role) bool {
	return role == protocol.RoleSensor || role == protocol.RoleConsumer
}
