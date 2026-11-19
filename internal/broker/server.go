package broker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
)

const (
	DefaultAddr = ":7000"

	// DefaultSensorQueueLimit limita os PUBLISH aceitos e ainda sem PUBACK de
	// cada sensor (RF-34). Ao lotar, só aquele sensor sofre backpressure.
	DefaultSensorQueueLimit = 32
	// DefaultSubscriberQueueLimit limita a retenção de cada assinante (RF-36).
	DefaultSubscriberQueueLimit = 10000
)

var (
	errNotListening = errors.New("broker: servidor nao esta escutando")
	errQueueLimit   = errors.New("broker: limite da fila do assinante deve ser >= 2")
)

type Config struct {
	Addr string
	// DataDir é o diretório do armazenamento em disco do broker
	// (inscrições, filas e cursores). Vazio mantém o estado só em memória,
	// sem sobreviver a reinício; serve apenas para testes.
	DataDir string
	// SensorQueueLimit é quantos PUBLISH de um sensor podem aguardar PUBACK.
	// Valores <= 0 usam DefaultSensorQueueLimit.
	SensorQueueLimit int
	// SubscriberQueueLimit é a retenção de cada assinante. Valores <= 0 usam
	// DefaultSubscriberQueueLimit; 1 é inválido (a cabeça é preservada).
	SubscriberQueueLimit int
	Logger               *slog.Logger
}

// QueueStats expõe profundidade e descartes das filas para métricas e testes.
type QueueStats struct {
	SensorQueues      map[string]int
	SubscriberQueues  map[string]int
	RetentionDrops    map[string]uint64
	BackpressureWaits uint64
}

type Server struct {
	cfg        Config
	log        *slog.Logger
	listener   net.Listener
	mu         sync.Mutex
	sessions   map[*session]struct{}
	topics     *TopicManager
	deliveries *DeliveryManager
	publisher  *publisher
	scheduler  *scheduler
	store      *store
	wg         sync.WaitGroup
	closeOnce  sync.Once
}

func NewServer(cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.SensorQueueLimit <= 0 {
		cfg.SensorQueueLimit = DefaultSensorQueueLimit
	}
	if cfg.SubscriberQueueLimit <= 0 {
		cfg.SubscriberQueueLimit = DefaultSubscriberQueueLimit
	}
	st := &store{}
	topics := NewTopicManager()
	deliveries := newDeliveryManager(st, cfg.Logger, cfg.SubscriberQueueLimit)
	pub := newPublisher(topics, deliveries, st)
	return &Server{
		cfg:        cfg,
		log:        cfg.Logger,
		sessions:   make(map[*session]struct{}),
		topics:     topics,
		deliveries: deliveries,
		publisher:  pub,
		scheduler:  newScheduler(pub.Publish),
		store:      st,
	}
}

// Listen recupera o estado persistido em Config.DataDir e abre o socket. O
// estado é carregado antes de aceitar conexões (RF-50).
func (s *Server) Listen() error {
	if s.cfg.SubscriberQueueLimit < 2 {
		return errQueueLimit
	}
	if err := s.openStore(); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		_ = s.store.close()
		return err
	}
	s.listener = ln
	return nil
}

func (s *Server) openStore() error {
	st, state, err := openStore(s.cfg.DataDir)
	if err != nil {
		return err
	}
	s.store = st
	s.deliveries.store = st
	s.publisher.store = st

	for clientID, filters := range state.subs {
		for filter := range filters {
			s.topics.Subscribe(clientID, filter)
		}
	}
	s.deliveries.restore(state)
	s.publisher.restore(state.sensors)

	if !st.persistent() {
		s.log.Warn("broker sem diretorio de dados: estado apenas em memoria")
		return nil
	}
	pending := 0
	for _, q := range state.queues {
		pending += q.len()
	}
	s.log.Info("estado recuperado", "data_dir", s.cfg.DataDir,
		"subscribers", len(state.subs), "pending", pending, "sensors", len(state.sensors))
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

	s.scheduler.start()
	s.log.Info("broker escutando", "addr", s.listener.Addr().String())

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return s.shutdown()
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return s.shutdown()
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

// shutdown espera as sessões e laços de entrega terminarem antes de fechar o
// store, para nenhuma gravação ficar pela metade.
func (s *Server) shutdown() error {
	s.wg.Wait()
	s.scheduler.close()
	s.deliveries.wait()
	return s.store.close()
}

// QueueStats devolve uma fotografia das filas: PUBLISH sem PUBACK por sensor,
// profundidade por assinante, descartes por retenção e esperas de backpressure.
func (s *Server) QueueStats() QueueStats {
	subscriberDepths, drops := s.deliveries.stats()
	return QueueStats{
		SensorQueues:      s.scheduler.depths(),
		SubscriberQueues:  subscriberDepths,
		RetentionDrops:    drops,
		BackpressureWaits: s.scheduler.waits.Load(),
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
