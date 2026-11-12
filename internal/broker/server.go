package broker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
)

const DefaultAddr = ":7000"

var errNotListening = errors.New("broker: servidor nao esta escutando")

type Config struct {
	Addr   string
	Logger *slog.Logger
}

type Server struct {
	cfg       Config
	log       *slog.Logger
	listener  net.Listener
	mu        sync.Mutex
	sessions  map[*session]struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewServer(cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{
		cfg:      cfg,
		log:      cfg.Logger,
		sessions: make(map[*session]struct{}),
	}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	return nil
}

func (s *Server) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	return s.Serve(ctx)
}

func (s *Server) Serve(ctx context.Context) error {
	if s.listener == nil {
		return errNotListening
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-stop:
		}
	}()

	s.log.Info("broker escutando", "addr", s.listener.Addr().String())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				s.wg.Wait()
				return nil
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return nil
			}
			s.log.Error("falha ao aceitar conexao", "error", err)
			continue
		}

		sess := newSession(s, conn)
		s.track(sess)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(sess)
			sess.serve(ctx)
		}()
	}
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		if s.listener != nil {
			_ = s.listener.Close()
		}

		s.mu.Lock()
		sessions := make([]*session, 0, len(s.sessions))
		for sess := range s.sessions {
			sessions = append(sessions, sess)
		}
		s.mu.Unlock()

		for _, sess := range sessions {
			_ = sess.close()
		}
	})
	return nil
}

func (s *Server) track(sess *session) {
	s.mu.Lock()
	s.sessions[sess] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) untrack(sess *session) {
	s.mu.Lock()
	delete(s.sessions, sess)
	s.mu.Unlock()
}
