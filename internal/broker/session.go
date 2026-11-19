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

var errInvalidFilter = errors.New("broker: filtro de topico invalido")

type session struct {
	server   *Server
	conn     net.Conn
	reader   *bufio.Reader
	clientID string
	role     protocol.Role
	writeMu  sync.Mutex
	log      *slog.Logger
}

func newSession(s *Server, conn net.Conn) *session {
	return &session{
		server: s,
		conn:   conn,
		reader: bufio.NewReader(conn),
	}
}

func (s *session) serve(ctx context.Context) {
	defer func() { _ = s.conn.Close() }()

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

	deliveries := s.server.deliveries
	deliveries.attach(s.clientID, s)
	defer deliveries.detach(s.clientID, s)

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
	return s.conn.Close()
}

func validRole(role protocol.Role) bool {
	return role == protocol.RoleSensor || role == protocol.RoleConsumer
}
