package consumer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/client"
)

// ManualConfig configura um consumidor controlado por comandos de texto,
// feito para demonstrar inscrição, cancelamento e retenção ao vivo.
type ManualConfig struct {
	Addr     string
	ClientID string
	// Timeout limita a conexão e cada SUBSCRIBE/UNSUBSCRIBE.
	Timeout   time.Duration
	Reconnect client.ReconnectOptions
	Logger    *slog.Logger
}

// DefaultManualClientID é o id padrão do consumidor manual.
const DefaultManualClientID = "consumidor-manual"

const manualTimeout = 5 * time.Second

const manualHelp = `comandos:
  inscrever <filtro>   assina um topico (ex.: sensores/sala-01/temperatura, sensores/+/status)
  cancelar <filtro>    cancela a assinatura; o que ja estava na fila ainda chega
  inscricoes           lista os filtros assinados nesta sessao
  pausar               para de consumir: a fila cresce no broker (e pode estourar a retencao)
  retomar              volta a consumir; o acumulado chega em ordem
  status               conexao, recebidas, repetidas e lacunas
  ajuda                mostra esta ajuda
  sair                 encerra (as inscricoes continuam gravadas no broker para este id)
`

// Manual é um consumidor dirigido por comandos (Run). Usa só a biblioteca
// cliente. As leituras aparecem no mesmo formato do -mostrar enquanto se
// digita.
type Manual struct {
	cfg     ManualConfig
	client  *client.Client
	tracker *Tracker

	outMu sync.Mutex
	out   io.Writer

	mu      sync.Mutex
	subs    map[string]struct{}
	paused  bool
	resumed chan struct{}
}

// NewManual valida a configuração. A conexão é feita em Run.
func NewManual(cfg ManualConfig) (*Manual, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("%w: endereco vazio", ErrInvalidConfig)
	}
	if cfg.ClientID == "" {
		cfg.ClientID = DefaultManualClientID
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = manualTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Manual{
		cfg:     cfg,
		tracker: NewTracker(0, 0),
		subs:    map[string]struct{}{},
		resumed: make(chan struct{}),
	}, nil
}

// Close desconecta.
func (m *Manual) Close() error {
	if m.client != nil {
		return m.client.Close()
	}
	return nil
}

// Run conecta, lê um comando por linha de in e escreve respostas e leituras
// em out, até "sair", fim da entrada ou ctx terminar.
func (m *Manual) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	m.out = out
	m.printf("consumidor %s -> %s\n", m.cfg.ClientID, m.cfg.Addr)
	connCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	c, err := client.ConnectWithOptions(connCtx, m.cfg.Addr, client.RoleConsumer, m.cfg.ClientID,
		client.Options{Reconnect: m.cfg.Reconnect})
	cancel()
	if err != nil {
		return fmt.Errorf("conectar a %s: %w", m.cfg.Addr, err)
	}
	m.client = c
	m.printf("  conectado. Inscricoes antigas deste id (de outras execucoes) continuam valendo no broker\n")
	m.printf("digite 'ajuda' para ver os comandos\n")

	delivered := make(chan struct{})
	go func() {
		defer close(delivered)
		m.deliverLoop(ctx)
	}()

	lines := make(chan string)
	readErr := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(in)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
		readErr <- sc.Err()
	}()

	for {
		m.printf("%s> ", m.cfg.ClientID)
		select {
		case <-ctx.Done():
			m.printf("\n")
			return nil
		case err := <-readErr:
			m.printf("\n")
			return err
		case <-delivered:
			m.printf("\n  cliente encerrado: %v\n", m.client.Err())
			return m.client.Err()
		case line := <-lines:
			if !m.handle(ctx, strings.Fields(line)) {
				return nil
			}
		}
	}
}

func (m *Manual) printf(format string, args ...any) {
	m.outMu.Lock()
	defer m.outMu.Unlock()
	_, _ = fmt.Fprintf(m.out, format, args...)
}

// deliverLoop mostra as entregas. Pausado, para de ler o canal: a biblioteca
// deixa de confirmar quando o canal enche e a fila cresce no broker.
func (m *Manual) deliverLoop(ctx context.Context) {
	for {
		m.mu.Lock()
		paused, resumed := m.paused, m.resumed
		m.mu.Unlock()
		if paused {
			select {
			case <-resumed:
				continue
			case <-ctx.Done():
				return
			}
		}
		select {
		case msg, ok := <-m.client.Messages():
			if !ok {
				return
			}
			obs := m.tracker.Observe(msg, time.Now())
			m.outMu.Lock()
			_, _ = fmt.Fprint(m.out, "\n")
			writeDisplay(m.out, msg, obs)
			m.outMu.Unlock()
		case <-resumed:
		case <-ctx.Done():
			return
		}
	}
}

// handle executa um comando; devolve false para encerrar.
func (m *Manual) handle(ctx context.Context, args []string) bool {
	if len(args) == 0 {
		return true
	}
	switch cmd := strings.ToLower(args[0]); cmd {
	case "sair", "exit", "quit":
		return false
	case "ajuda", "help", "?":
		m.printf("%s", manualHelp)
	case "inscrever", "assinar":
		if filter, ok := m.filterArg(cmd, args); ok {
			m.subscribe(ctx, filter)
		}
	case "cancelar", "desinscrever":
		if filter, ok := m.filterArg(cmd, args); ok {
			m.unsubscribe(ctx, filter)
		}
	case "inscricoes":
		m.listSubs()
	case "pausar":
		m.setPaused(true)
	case "retomar":
		m.setPaused(false)
	case "status":
		m.status()
	default:
		m.printf("  comando desconhecido %q; digite 'ajuda'\n", args[0])
	}
	return true
}

func (m *Manual) filterArg(cmd string, args []string) (string, bool) {
	if len(args) != 2 {
		m.printf("  uso: %s <filtro>, por exemplo: %s sensores/+/temperatura\n", cmd, cmd)
		return "", false
	}
	if !validFilter(args[1]) {
		m.printf("  filtro invalido %q: niveis separados por /, sem nivel vazio; + ocupa um nivel inteiro; # nao e suportado\n", args[1])
		return "", false
	}
	if !strings.HasPrefix(args[1], "sensores/") {
		m.printf("  aviso: os sensores publicam em sensores/<id>/temperatura e sensores/<id>/status; %q nao casa com nenhum deles\n", args[1])
	}
	return args[1], true
}

// validFilter segue as regras do broker (ValidFilter): o broker encerra a
// conexão de quem manda filtro inválido, então o erro é pego antes.
func validFilter(filter string) bool {
	if filter == "" {
		return false
	}
	for _, level := range strings.Split(filter, "/") {
		if level == "" || strings.Contains(level, "#") || (strings.Contains(level, "+") && level != "+") {
			return false
		}
	}
	return true
}

func (m *Manual) subscribe(ctx context.Context, filter string) {
	opCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	if err := m.client.Subscribe(opCtx, filter); err != nil {
		m.printf("  inscricao falhou: %v\n", describeErr(err))
		return
	}
	m.mu.Lock()
	m.subs[filter] = struct{}{}
	m.mu.Unlock()
	m.printf("  inscrito em %s: so chegam leituras publicadas a partir de agora (sem replay)\n", filter)
}

func (m *Manual) unsubscribe(ctx context.Context, filter string) {
	opCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()
	if err := m.client.Unsubscribe(opCtx, filter); err != nil {
		m.printf("  cancelamento falhou: %v\n", describeErr(err))
		return
	}
	m.mu.Lock()
	_, had := m.subs[filter]
	delete(m.subs, filter)
	m.mu.Unlock()
	m.printf("  inscricao em %s cancelada: novas leituras nao chegam; as que ja estavam na fila ainda chegam\n", filter)
	if !had {
		m.printf("  (nao estava na lista desta sessao; se era de uma execucao anterior com este id, foi cancelada tambem)\n")
	}
	m.printf("  se ninguem mais assina esse topico, o sensor volta a receber NO_SUBSCRIBERS\n")
}

func (m *Manual) listSubs() {
	m.mu.Lock()
	filters := make([]string, 0, len(m.subs))
	for f := range m.subs {
		filters = append(filters, f)
	}
	m.mu.Unlock()
	sort.Strings(filters)
	if len(filters) == 0 {
		m.printf("  nenhuma inscricao feita nesta sessao\n")
	}
	for _, f := range filters {
		m.printf("  %s\n", f)
	}
	m.printf("  (o protocolo nao permite listar o que o broker guarda; inscricoes de execucoes anteriores deste id nao aparecem aqui)\n")
}

func (m *Manual) setPaused(paused bool) {
	m.mu.Lock()
	if m.paused == paused {
		m.mu.Unlock()
		if paused {
			m.printf("  ja pausado\n")
		} else {
			m.printf("  ja consumindo\n")
		}
		return
	}
	m.paused = paused
	if !paused {
		close(m.resumed)
		m.resumed = make(chan struct{})
	}
	m.mu.Unlock()
	if paused {
		m.printf("  pausado: as leituras se acumulam (ate 64 na biblioteca, depois na fila deste consumidor no broker)\n")
		return
	}
	m.printf("  retomando: o acumulado chega em ordem; se a fila estourou, aparece o aviso de perda por retencao\n")
}

func (m *Manual) status() {
	st := m.tracker.Stats()
	conn := "conectado"
	if !m.client.Connected() {
		conn = "reconectando"
	}
	m.mu.Lock()
	state := "consumindo"
	if m.paused {
		state = "pausado"
	}
	m.mu.Unlock()
	m.printf("  %s a %s | %s | recebidas: %d | repetidas: %d | lacunas: %d sem marca, %d por retencao\n",
		conn, m.cfg.Addr, state, st.Received, st.Duplicates, st.GapsUnmarked, st.GapsMarked)
}

func describeErr(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "tempo esgotado (broker sem resposta)"
	case errors.Is(err, client.ErrDisconnected):
		return "conexao caiu; tente de novo"
	default:
		return err.Error()
	}
}
