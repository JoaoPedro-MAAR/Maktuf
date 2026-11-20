// Package vip simula o endereço virtual do par de brokers (task 013, RF-44,
// RF-49). Os clientes conectam sempre no endereço do vip; ele repassa cada
// conexão ao broker que aceita o CONNECT. A reserva e o primário isolado
// respondem CONNACK(TEMPORARY_FAILURE), então só o primário atual recebe
// clientes, sem que o vip precise falar com o Juiz.
//
// Depois do CONNACK, o vip só copia bytes nos dois sentidos, sem interpretar
// o protocolo. Quando a conexão com o broker cai (primário morto ou isolado),
// a do cliente é fechada junto e a biblioteca cliente reconecta. Num IP
// virtual real esse papel é da rede; as consequências da simulação estão em
// docs/ressalvas.md.
package vip

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/metrics"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

const (
	DefaultAddr             = ":7070"
	DefaultDialTimeout      = time.Second
	DefaultHandshakeTimeout = 5 * time.Second
)

var (
	ErrInvalidConfig = errors.New("vip: configuracao invalida")
	errNotListening  = errors.New("vip: nao esta escutando")
	errNoPrimary     = errors.New("vip: nenhum broker aceitou o CONNECT")
)

// Config configura o vip. Campos de duração zerados recebem os defaults.
type Config struct {
	// Addr é o endereço que os clientes usam.
	Addr string
	// Backends são os endereços dos brokers, na ordem de tentativa.
	Backends []string
	// DialTimeout limita a conexão com cada broker.
	DialTimeout time.Duration
	// HandshakeTimeout limita a espera pelo CONNECT do cliente e pelo
	// CONNACK de cada broker.
	HandshakeTimeout time.Duration
	Metrics          *metrics.Registry
	Logger           *slog.Logger
}

// Proxy é o endereço virtual.
type Proxy struct {
	cfg     Config
	log     *slog.Logger
	ln      net.Listener
	metrics *vipMetrics

	preferred atomic.Value // string: último broker que aceitou
	pairs     atomic.Int64

	mu   sync.Mutex
	open map[net.Conn]struct{}
	wg   sync.WaitGroup
}

// New valida a configuração e prepara o vip.
func New(cfg Config) (*Proxy, error) {
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("%w: nenhum broker em Backends", ErrInvalidConfig)
	}
	for _, b := range cfg.Backends {
		if b == "" {
			return nil, fmt.Errorf("%w: endereco de broker vazio", ErrInvalidConfig)
		}
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewRegistry()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	cfg.Backends = append([]string(nil), cfg.Backends...)
	p := &Proxy{cfg: cfg, log: cfg.Logger, open: make(map[net.Conn]struct{})}
	p.preferred.Store("")
	p.metrics = newVipMetrics(cfg.Metrics, p)
	return p, nil
}

// Metrics devolve o registro com as métricas do vip.
func (p *Proxy) Metrics() *metrics.Registry { return p.cfg.Metrics }

// Listen abre o endereço virtual.
func (p *Proxy) Listen() error {
	ln, err := net.Listen("tcp", p.cfg.Addr)
	if err != nil {
		return err
	}
	p.ln = ln
	return nil
}

// Addr devolve o endereço em que o vip escuta; nil antes de Listen.
func (p *Proxy) Addr() net.Addr {
	if p.ln == nil {
		return nil
	}
	return p.ln.Addr()
}

// ListenAndServe é Listen seguido de Serve.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	if err := p.Listen(); err != nil {
		return err
	}
	return p.Serve(ctx)
}

// Serve aceita clientes até ctx terminar; então fecha o listener e todas as
// conexões, espera as goroutines e devolve nil.
func (p *Proxy) Serve(ctx context.Context) error {
	if p.ln == nil {
		return errNotListening
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
		case <-stop:
			return
		}
		_ = p.ln.Close()
		p.mu.Lock()
		for conn := range p.open {
			_ = conn.Close()
		}
		p.mu.Unlock()
	}()

	p.log.Info("vip escutando", "addr", p.ln.Addr().String(), "backends", p.cfg.Backends)
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				p.wg.Wait()
				return nil
			}
			p.log.Error("falha ao aceitar conexao", "error", err)
			continue
		}
		if !p.track(ctx, conn) {
			continue
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.untrack(conn)
			p.handle(ctx, conn)
		}()
	}
}

// track registra a conexão para o encerramento. Depois do cancelamento, a
// conexão é fechada na hora: o laço de encerramento já passou.
func (p *Proxy) track(ctx context.Context, conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ctx.Err() != nil {
		_ = conn.Close()
		return false
	}
	p.open[conn] = struct{}{}
	return true
}

func (p *Proxy) untrack(conn net.Conn) {
	p.mu.Lock()
	delete(p.open, conn)
	p.mu.Unlock()
	_ = conn.Close()
}

// handle lê o CONNECT do cliente, escolhe o broker e repassa os bytes.
func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	log := p.log.With("remote_addr", client.RemoteAddr().String())
	cr := bufio.NewReader(client)
	if err := client.SetDeadline(time.Now().Add(p.cfg.HandshakeTimeout)); err != nil {
		return
	}
	msg, err := protocol.ReadMessage(cr)
	if err != nil {
		log.Debug("falha ao ler o CONNECT", "error", err)
		return
	}
	connect, ok := msg.(*protocol.Connect)
	if !ok {
		log.Warn("primeira mensagem nao e CONNECT", "type", msg.Type().String())
		return
	}
	log = log.With("client_id", connect.ClientID)

	backend, br, addr, err := p.route(connect)
	if err != nil {
		p.metrics.rejected.Inc()
		log.Warn("CONNECT recusado: nenhum broker aceitou", "error", err)
		_ = protocol.WriteMessage(client, &protocol.ConnAck{Status: protocol.StatusTemporaryFailure})
		return
	}
	if !p.track(ctx, backend) {
		return
	}
	defer p.untrack(backend)

	if err := protocol.WriteMessage(client, &protocol.ConnAck{Status: protocol.StatusOK}); err != nil {
		return
	}
	if client.SetDeadline(time.Time{}) != nil || backend.SetDeadline(time.Time{}) != nil {
		return
	}
	p.metrics.routed.Inc(addr)
	log.Info("cliente encaminhado", "backend", addr)

	p.pairs.Add(1)
	defer p.pairs.Add(-1)
	pipe(client, cr, backend, br)
	log.Debug("conexao encerrada", "backend", addr)
}

// route tenta os brokers até um aceitar o CONNECT e devolve a conexão já
// autenticada, o reader dela (pode conter bytes depois do CONNACK) e o
// endereço escolhido.
func (p *Proxy) route(connect *protocol.Connect) (net.Conn, *bufio.Reader, string, error) {
	last := errNoPrimary
	for _, addr := range p.order() {
		conn, br, err := p.tryBackend(addr, connect)
		if err != nil {
			last = fmt.Errorf("%s: %w", addr, err)
			continue
		}
		p.preferred.Store(addr)
		return conn, br, addr, nil
	}
	return nil, nil, "", last
}

func (p *Proxy) tryBackend(addr string, connect *protocol.Connect) (net.Conn, *bufio.Reader, error) {
	conn, err := net.DialTimeout("tcp", addr, p.cfg.DialTimeout)
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (net.Conn, *bufio.Reader, error) {
		_ = conn.Close()
		return nil, nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(p.cfg.HandshakeTimeout)); err != nil {
		return fail(err)
	}
	if err := protocol.WriteMessage(conn, connect); err != nil {
		return fail(err)
	}
	br := bufio.NewReader(conn)
	msg, err := protocol.ReadMessage(br)
	if err != nil {
		return fail(err)
	}
	ack, ok := msg.(*protocol.ConnAck)
	if !ok {
		return fail(fmt.Errorf("esperava CONNACK, recebeu %s", msg.Type()))
	}
	if ack.Status != protocol.StatusOK {
		return fail(fmt.Errorf("CONNACK %s", ack.Status))
	}
	return conn, br, nil
}

// order devolve o último broker que aceitou e depois os demais, na ordem da
// configuração.
func (p *Proxy) order() []string {
	pref, _ := p.preferred.Load().(string)
	out := make([]string, 0, len(p.cfg.Backends))
	if pref != "" {
		out = append(out, pref)
	}
	for _, b := range p.cfg.Backends {
		if b != pref {
			out = append(out, b)
		}
	}
	return out
}

// pipe copia nos dois sentidos e fecha as duas conexões quando qualquer lado
// termina: é o que derruba o cliente quando o broker cai.
func pipe(client net.Conn, cr *bufio.Reader, backend net.Conn, br *bufio.Reader) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(backend, cr)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, br)
		done <- struct{}{}
	}()
	<-done
	_ = client.Close()
	_ = backend.Close()
	<-done
}
