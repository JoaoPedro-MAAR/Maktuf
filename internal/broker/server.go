package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/replication"
)

const (
	DefaultAddr = ":7000"

	// DefaultSensorQueueLimit limita os PUBLISH aceitos e ainda sem PUBACK de
	// cada sensor (RF-34). Ao lotar, só aquele sensor sofre backpressure.
	DefaultSensorQueueLimit = 32
	// DefaultSubscriberQueueLimit limita a retenção de cada assinante (RF-36).
	DefaultSubscriberQueueLimit = 10000

	// DefaultHeartbeatInterval é o período do HEARTBEAT ao Juiz.
	DefaultHeartbeatInterval = time.Second
	// DefaultFenceTimeout é quanto o primário espera o aval do Juiz para
	// confirmar uma escrita sem reserva antes de se isolar. Deve ser menor
	// que o prazo de falha do Juiz.
	DefaultFenceTimeout = 3 * time.Second
)

// Role é o papel do broker no par replicado (DP-12).
type Role string

const (
	// RolePrimary atende clientes e replica o store na reserva.
	RolePrimary Role = "primario"
	// RoleBackup só recebe a replicação e recusa clientes até ser promovida.
	RoleBackup Role = "reserva"
)

var (
	errNotListening = errors.New("broker: servidor nao esta escutando")
	errQueueLimit   = errors.New("broker: limite da fila do assinante deve ser >= 2")
	errInvalidRole  = errors.New("broker: papel invalido")
	errReplication  = errors.New("broker: configuracao de replicacao invalida")
	errJudgeConfig  = errors.New("broker: configuracao do juiz invalida")
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
	// StatusTolerance é somada à duração do SLEEP antes de marcar OFFLINE.
	// Valores <= 0 usam DefaultStatusTolerance.
	StatusTolerance time.Duration
	// StatusScanInterval é o período do monitor de prazos de sono. Valores <= 0
	// usam DefaultStatusScanInterval.
	StatusScanInterval time.Duration
	// Role é o papel do broker. Vazio equivale a RolePrimary.
	Role Role
	// ReplicaAddr é o endereço de replicação da reserva, usado pelo primário.
	// Vazio desliga a replicação (broker sozinho).
	ReplicaAddr string
	// ReplicationAddr é onde a reserva escuta a replicação do primário.
	ReplicationAddr string
	// ReplicaTimeout limita a conexão com a reserva e a espera de cada
	// REPLICA_ACK. Valores <= 0 usam replication.DefaultTimeout.
	ReplicaTimeout time.Duration
	// BrokerID identifica o broker para o Juiz. Obrigatório com JudgeAddr.
	BrokerID string
	// JudgeAddr é o endereço do Juiz. Vazio desliga heartbeat, promoção e
	// isolamento (task 012).
	JudgeAddr string
	// HeartbeatInterval é o período do HEARTBEAT. Valores <= 0 usam
	// DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration
	// FenceTimeout limita a espera pelo aval do Juiz no modo sem reserva.
	// Valores <= 0 usam DefaultFenceTimeout.
	FenceTimeout time.Duration
	// Metrics recebe as métricas do broker. Nil cria um registro próprio,
	// acessível por Server.Metrics.
	Metrics *metrics.Registry
	Logger  *slog.Logger
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
	status     *statusManager
	monitor    *monitor
	store      *store
	primary    *replication.Primary
	registry   *metrics.Registry
	metrics    *brokerMetrics
	wg         sync.WaitGroup
	closeOnce  sync.Once

	// Estado de alta disponibilidade (task 012). O papel muda de reserva
	// para primário na promoção; haMu serializa a promoção e o acesso à
	// reserva.
	role    atomic.Value
	fenced  atomic.Bool
	deposed atomic.Bool
	judge   *judgeLink
	haMu    sync.Mutex
	replica *replication.Replica
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
	if cfg.StatusTolerance <= 0 {
		cfg.StatusTolerance = DefaultStatusTolerance
	}
	if cfg.StatusScanInterval <= 0 {
		cfg.StatusScanInterval = DefaultStatusScanInterval
	}
	if cfg.Role == "" {
		cfg.Role = RolePrimary
	}
	if cfg.ReplicaTimeout <= 0 {
		cfg.ReplicaTimeout = replication.DefaultTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.FenceTimeout <= 0 {
		cfg.FenceTimeout = DefaultFenceTimeout
	}
	st := &store{}
	topics := NewTopicManager()
	deliveries := newDeliveryManager(st, cfg.Logger, cfg.SubscriberQueueLimit)
	pub := newPublisher(topics, deliveries, st)
	status := newStatusManager(cfg.StatusTolerance, pub.PublishStatus, cfg.Logger)
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	s := &Server{
		cfg:        cfg,
		log:        cfg.Logger,
		sessions:   make(map[*session]struct{}),
		topics:     topics,
		deliveries: deliveries,
		publisher:  pub,
		scheduler:  newScheduler(pub.Publish),
		status:     status,
		monitor:    newMonitor(status, cfg.StatusScanInterval),
		store:      st,
		registry:   cfg.Metrics,
	}
	s.role.Store(cfg.Role)
	if cfg.JudgeAddr != "" {
		// Criado aqui, e não em Listen, para as métricas lerem o campo sem
		// corrida com a inicialização.
		s.judge = newJudgeLink(s)
	}
	s.metrics = newBrokerMetrics(cfg.Metrics, s)
	deliveries.onSend = s.metrics.deliverySent
	deliveries.onAck = s.metrics.deliveryAcked
	return s
}

// Metrics devolve o registro com as métricas do broker (seção 9 de
// requisitos.md), para expor em /metrics.
func (s *Server) Metrics() *metrics.Registry {
	return s.registry
}

// Listen recupera o estado persistido em Config.DataDir e abre o socket. O
// estado é carregado antes de aceitar conexões (RF-50). Na reserva, também
// abre o endereço de replicação.
func (s *Server) Listen() error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := s.openStore(); err != nil {
		return err
	}
	if s.cfg.Role == RoleBackup {
		replica, err := replication.Listen(s.cfg.ReplicationAddr, replicaTarget{s.store}, s.log)
		if err != nil {
			_ = s.store.close()
			return err
		}
		s.replica = replica
	}
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		if s.replica != nil {
			_ = s.replica.Close()
		}
		_ = s.store.close()
		return err
	}
	s.listener = ln
	if s.cfg.Role == RolePrimary && s.cfg.ReplicaAddr != "" {
		pcfg := replication.PrimaryConfig{
			Addr:    s.cfg.ReplicaAddr,
			Timeout: s.cfg.ReplicaTimeout,
			Logger:  s.log,
		}
		if s.judge != nil {
			pcfg.ConfirmAlone = s.judge.confirmAlone
			pcfg.OnChange = s.replicaChanged
		}
		s.primary = replication.NewPrimary(pcfg)
		s.store.replicator = s.primary
	}
	return nil
}

func (s *Server) validate() error {
	if s.cfg.SubscriberQueueLimit < 2 {
		return errQueueLimit
	}
	switch s.cfg.Role {
	case RolePrimary:
		if s.cfg.ReplicaAddr != "" && s.cfg.DataDir == "" {
			return fmt.Errorf("%w: o primario com reserva exige DataDir", errReplication)
		}
	case RoleBackup:
		if s.cfg.DataDir == "" || s.cfg.ReplicationAddr == "" {
			return fmt.Errorf("%w: a reserva exige DataDir e ReplicationAddr", errReplication)
		}
	default:
		return fmt.Errorf("%w: %q", errInvalidRole, s.cfg.Role)
	}
	if s.cfg.JudgeAddr != "" && s.cfg.BrokerID == "" {
		return fmt.Errorf("%w: o juiz exige BrokerID", errJudgeConfig)
	}
	if s.cfg.BrokerID == protocol.JudgeID {
		return fmt.Errorf("%w: BrokerID %q e reservado ao juiz", errJudgeConfig, protocol.JudgeID)
	}
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

	if s.cfg.Role == RoleBackup {
		// A reserva não atende clientes: o estado fica só no store e é
		// montado em memória quando ela for promovida (promote).
		s.log.Info("reserva com estado em disco", "data_dir", s.cfg.DataDir, "sensors", len(state.sensors))
		return nil
	}

	s.loadState(state)

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

// loadState monta em memória inscrições, filas, cursores e o último seq de
// cada sensor a partir do estado do store. Usada ao abrir o primário e na
// promoção da reserva.
func (s *Server) loadState(state *storeState) {
	for clientID, filters := range state.subs {
		for filter := range filters {
			s.topics.Subscribe(clientID, filter)
		}
	}
	s.deliveries.restore(state)
	s.publisher.restore(state.sensors)
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
	s.monitor.start()
	if s.primary != nil {
		s.primary.Start(replicaSource{s.store})
	}
	if s.replica != nil {
		s.replica.Start()
	}
	if s.judge != nil {
		s.judge.start()
	}
	s.log.Info("broker escutando", "addr", s.listener.Addr().String(), "role", string(s.Role()))

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
	// O link com o Juiz para primeiro: nenhuma promoção começa durante o
	// encerramento.
	if s.judge != nil {
		s.judge.close()
	}
	s.wg.Wait()
	s.scheduler.close()
	s.monitor.close()
	s.deliveries.wait()
	if s.primary != nil {
		_ = s.primary.Close()
	}
	s.haMu.Lock()
	if s.replica != nil {
		_ = s.replica.Close()
		s.replica = nil
	}
	s.haMu.Unlock()
	return s.store.close()
}

// Role devolve o papel configurado do broker.
func (s *Server) Role() Role {
	return s.role.Load().(Role)
}

// ReplicaSynced informa se a reserva está sincronizada com este primário.
// Falso sem reserva configurada ou fora do papel de primário.
func (s *Server) ReplicaSynced() bool {
	return s.primary != nil && s.primary.Synced()
}

// ReplicaDesyncs conta quantas vezes a reserva deixou de estar sincronizada.
func (s *Server) ReplicaDesyncs() uint64 {
	if s.primary == nil {
		return 0
	}
	return s.primary.Desyncs()
}

// ReplicationAddr devolve o endereço de replicação da reserva; nil fora do
// papel de reserva ou antes de Listen.
func (s *Server) ReplicationAddr() net.Addr {
	s.haMu.Lock()
	defer s.haMu.Unlock()
	if s.replica == nil {
		return nil
	}
	return s.replica.Addr()
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
		s.closeSessions()
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
