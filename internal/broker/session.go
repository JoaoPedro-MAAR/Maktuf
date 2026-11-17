package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

var errInvalidFilter = errors.New("broker: filtro de topico invalido")

type session struct {
	server   *Server
	conn     net.Conn
	reader   *bufio.Reader
	clientID string
	role     protocol.Role
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

	if err := protocol.WriteMessage(s.conn, &protocol.ConnAck{Status: protocol.StatusOK}); err != nil {
		log.Warn("falha ao enviar CONNACK", "error", err)
		return
	}

	s.clientID = connect.ClientID
	s.role = connect.Role
	log.Info("cliente conectado")

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
	created := s.server.topics.Subscribe(s.clientID, m.TopicFilter)
	if err := protocol.WriteMessage(s.conn, &protocol.SubAck{Status: protocol.StatusOK}); err != nil {
		return err
	}
	log.Info("inscricao registrada", "topic_filter", m.TopicFilter, "topic_created", created)
	return nil
}

func (s *session) handleUnsubscribe(log *slog.Logger, m *protocol.Unsubscribe) error {
	if !ValidFilter(m.TopicFilter) {
		return fmt.Errorf("%w: %q", errInvalidFilter, m.TopicFilter)
	}
	removed := s.server.topics.Unsubscribe(s.clientID, m.TopicFilter)
	if err := protocol.WriteMessage(s.conn, &protocol.UnsubAck{Status: protocol.StatusOK}); err != nil {
		return err
	}
	log.Info("inscricao removida", "topic_filter", m.TopicFilter, "topic_removed", removed)
	return nil
}

func (s *session) close() error {
	return s.conn.Close()
}

func validRole(role protocol.Role) bool {
	return role == protocol.RoleSensor || role == protocol.RoleConsumer
}
