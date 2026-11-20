// Package client é a biblioteca de comunicação do Maktuf. Ela esconde o
// socket TCP, o enquadramento binário do protocolo (docs/protocolo.md), a
// reconexão automática e o reenvio do buffer durável do sensor atrás das
// operações Connect, Publish, Subscribe, Unsubscribe, Sleep e Close (RF-51,
// RF-53, RF-54).
//
// Uso por um sensor:
//
//	c, err := client.Connect(ctx, "127.0.0.1:7000", client.RoleSensor, "sensor-a")
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	status, err := c.Publish(ctx, "sensores/a/temperatura", client.Reading{
//		SensorID:    "sensor-a",
//		Seq:         1,
//		Age:         2 * time.Second,
//		Temperature: 25.5,
//	})
//	// status: client.StatusPersisted, client.StatusNoSubscribers ou
//	// client.StatusTemporaryFailure (RF-52).
//
// Uso por um consumidor:
//
//	c, err := client.Connect(ctx, "127.0.0.1:7000", client.RoleConsumer, "consumidor-1")
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	if err := c.Subscribe(ctx, "sensores/+/temperatura"); err != nil {
//		return err
//	}
//	for msg := range c.Messages() {
//		fmt.Println(msg.SensorID, msg.Seq, msg.Temperature, msg.Timestamp)
//	}
//
// Todos os métodos podem ser chamados concorrentemente por várias goroutines
// (RF-55). Quando a conexão cai, o cliente reconecta com backoff e restaura as
// inscrições ativas; Done só é fechado em Close ou quando as tentativas de
// reconexão se esgotam.
package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
)

type Role = protocol.Role

const (
	RoleSensor   = protocol.RoleSensor
	RoleConsumer = protocol.RoleConsumer
)

type Status = protocol.Status

const (
	StatusOK               = protocol.StatusOK
	StatusPersisted        = protocol.StatusPersisted
	StatusNoSubscribers    = protocol.StatusNoSubscribers
	StatusTemporaryFailure = protocol.StatusTemporaryFailure
)

type SensorState = protocol.SensorState

const (
	SensorStateUnknown  = protocol.SensorStateUnknown
	SensorStateOnline   = protocol.SensorStateOnline
	SensorStateDormindo = protocol.SensorStateDormindo
	SensorStateOffline  = protocol.SensorStateOffline
)

var (
	// ErrClosed indica que Close foi chamado.
	ErrClosed = errors.New("client: cliente fechado")

	// ErrDisconnected indica que a conexão com o broker foi perdida. Durante a
	// reconexão automática, operações em andamento podem devolver este erro.
	ErrDisconnected = errors.New("client: conexao com o broker perdida")

	// ErrHandshake indica que o broker recusou ou não concluiu o CONNECT.
	ErrHandshake = errors.New("client: handshake CONNECT/CONNACK falhou")

	// ErrRejected indica que o broker respondeu com status diferente de OK.
	ErrRejected = errors.New("client: operacao recusada pelo broker")

	// ErrUnexpectedMessage indica uma resposta fora do protocolo; a conexão é
	// encerrada porque o fluxo deixou de ser confiável.
	ErrUnexpectedMessage = errors.New("client: mensagem inesperada do broker")

	// ErrReconnectExhausted indica que MaxAttempts foi atingido sem reconectar.
	ErrReconnectExhausted = errors.New("client: tentativas de reconexao esgotadas")

	// ErrTemporaryFailure indica que o broker pediu retry mantendo a leitura no
	// buffer (protocolo, status TEMPORARY_FAILURE).
	ErrTemporaryFailure = errors.New("client: falha temporaria; leitura mantida no buffer")

	ErrInvalidArgument = errors.New("client: argumento invalido")
)

// Reading é uma leitura de temperatura publicada por um sensor. Age é há
// quanto tempo a leitura foi feita; o broker calcula o instante (DP-10).
type Reading struct {
	SensorID    string
	Seq         uint64
	Age         time.Duration
	Temperature float32
}

// Message é uma leitura entregue pelo broker a um consumidor.
type Message struct {
	Topic       string
	SensorID    string
	Seq         uint64
	Timestamp   time.Time
	Temperature float32
	// Gap indica que leituras deste sensor foram descartadas por retenção
	// antes desta entrega (RF-37). O seq anterior conhecido do consumidor
	// ajuda a dimensionar a lacuna.
	Gap bool
	// Status diferente de SensorStateUnknown marca uma transição de estado
	// publicada no tópico sensores/{id}/status (RF-21). Nesse caso Temperature
	// não tem significado.
	Status SensorState
}

const messageBuffer = 64

type waiterKind int

const (
	waitPubAck waiterKind = iota
	waitSubAck
	waitUnsubAck
	waiterKinds
)

type response struct {
	msg protocol.Message
	err error
}

type waiter struct {
	ch  chan response
	seq uint64
}

// Client é uma conexão com o broker. A escrita é serializada por writeMu; a
// leitura é feita por uma goroutine por conexão que roteia respostas e
// DELIVERs. O estado da conexão é protegido por mu.
//
// Como SUBACK e UNSUBACK não identificam o pedido, as respostas são
// correlacionadas pela ordem: o broker trata as mensagens de uma sessão em
// sequência, e o waiter é enfileirado sob writeMu junto com a escrita.
type Client struct {
	addr     string
	clientID string
	role     Role
	opts     Options

	ctx    context.Context
	cancel context.CancelFunc

	writeMu sync.Mutex

	mu           sync.Mutex
	conn         net.Conn
	restoring    bool
	stateChanged chan struct{}
	terminal     error
	pending      [waiterKinds][]waiter
	inbox        []Message
	subs         map[string]struct{}

	inboxSignal chan struct{}
	done        chan struct{}
	messages    chan Message
	wg          sync.WaitGroup

	sensor *sensorBuffer
}

// Connect abre a conexão TCP, envia CONNECT e valida o CONNACK. O contexto
// limita o tempo de conexão e do handshake. Quedas posteriores são tratadas
// pela reconexão automática com os parâmetros padrão.
func Connect(ctx context.Context, addr string, role Role, clientID string) (*Client, error) {
	return ConnectWithOptions(ctx, addr, role, clientID, Options{})
}

func newClient(addr string, role Role, clientID string, opts Options) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		addr:         addr,
		clientID:     clientID,
		role:         role,
		opts:         opts,
		ctx:          ctx,
		cancel:       cancel,
		stateChanged: make(chan struct{}),
		subs:         make(map[string]struct{}),
		inboxSignal:  make(chan struct{}, 1),
		done:         make(chan struct{}),
		messages:     make(chan Message, messageBuffer),
	}
	if opts.Sensor != nil {
		c.sensor = &sensorBuffer{
			buffer:   opts.Sensor.Buffer,
			topic:    opts.Sensor.Topic,
			sensorID: opts.Sensor.SensorID,
		}
	}
	return c
}

func handshake(ctx context.Context, conn net.Conn, r *bufio.Reader, role Role, clientID string) error {
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	if err := protocol.WriteMessage(conn, &protocol.Connect{Role: role, ClientID: clientID}); err != nil {
		return ctxErr(ctx, err)
	}
	msg, err := protocol.ReadMessage(r)
	if err != nil {
		return ctxErr(ctx, err)
	}
	ack, ok := msg.(*protocol.ConnAck)
	if !ok {
		return fmt.Errorf("%w: esperava CONNACK, recebeu %s", ErrUnexpectedMessage, msg.Type())
	}
	if ack.Status != protocol.StatusOK {
		return fmt.Errorf("%w: status %s", ErrRejected, ack.Status)
	}
	if !stop() {
		return ctx.Err()
	}
	return conn.SetDeadline(time.Time{})
}

func ctxErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func (c *Client) ClientID() string { return c.clientID }

// Messages devolve o canal de leituras entregues pelo broker. O DELIVER_ACK é
// enviado quando a leitura é colocada no canal; a entrega é at-least-once e
// a mesma leitura pode chegar mais de uma vez. O canal só é fechado no
// encerramento final do cliente; quedas transitórias são escondidas.
func (c *Client) Messages() <-chan Message { return c.messages }

// Done é fechado quando o cliente termina definitivamente, por Close ou por
// esgotamento das tentativas de reconexão.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err devolve a causa do encerramento final, ou nil enquanto o cliente
// está ativo ou reconectando.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminal
}

// Connected informa se há uma conexão pronta, com o CONNACK concluído e as
// inscrições restauradas.
func (c *Client) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.restoring && c.terminal == nil
}

// Publish envia a leitura e devolve o status do PUBACK (RF-52).
func (c *Client) Publish(ctx context.Context, topic string, r Reading) (Status, error) {
	if r.SensorID == "" {
		return 0, fmt.Errorf("%w: sensor_id vazio", ErrInvalidArgument)
	}
	if r.Age < 0 || r.Age.Milliseconds() > math.MaxUint32 {
		return 0, fmt.Errorf("%w: idade %s fora do intervalo", ErrInvalidArgument, r.Age)
	}

	msg, err := c.request(ctx, waitPubAck, r.Seq, &protocol.Publish{
		Topic:       topic,
		SensorID:    r.SensorID,
		Seq:         r.Seq,
		AgeMS:       uint32(r.Age.Milliseconds()),
		Temperature: r.Temperature,
	})
	if err != nil {
		return 0, err
	}
	return msg.(*protocol.PubAck).Status, nil
}

// Sleep avisa o broker que o sensor ficará fora por duration. O SLEEP não tem
// resposta no protocolo; o broker marca o sensor como DORMINDO.
func (c *Client) Sleep(ctx context.Context, duration time.Duration) error {
	if duration < 0 || duration.Milliseconds() > math.MaxUint32 {
		return fmt.Errorf("%w: duracao %s fora do intervalo", ErrInvalidArgument, duration)
	}

	msg := &protocol.Sleep{DurationMS: uint32(duration.Milliseconds())}
	for {
		conn, err := c.waitConnected(ctx)
		if err != nil {
			return err
		}

		c.writeMu.Lock()
		c.mu.Lock()
		if c.terminal != nil {
			err := c.terminal
			c.mu.Unlock()
			c.writeMu.Unlock()
			return err
		}
		if c.conn != conn {
			c.mu.Unlock()
			c.writeMu.Unlock()
			continue
		}
		c.mu.Unlock()
		err = c.writeLocked(ctx, conn, msg)
		c.writeMu.Unlock()

		if err == nil {
			return nil
		}
		c.connectionLost(conn, fmt.Errorf("envio de %s: %w", msg.Type(), err))
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if term := c.Err(); term != nil {
			return term
		}
	}
}

// Subscribe registra o filtro e aguarda o SUBACK. Inscrições ativas são
// restauradas automaticamente depois de uma reconexão (RF-54).
func (c *Client) Subscribe(ctx context.Context, filter string) error {
	msg, err := c.request(ctx, waitSubAck, 0, &protocol.Subscribe{TopicFilter: filter})
	if err != nil {
		return err
	}
	if status := msg.(*protocol.SubAck).Status; status != protocol.StatusOK {
		return fmt.Errorf("%w: SUBACK %s", ErrRejected, status)
	}
	c.mu.Lock()
	c.subs[filter] = struct{}{}
	c.mu.Unlock()
	return nil
}

// Unsubscribe remove o filtro e aguarda o UNSUBACK. Leituras já enfileiradas
// no broker antes do cancelamento ainda podem ser entregues.
func (c *Client) Unsubscribe(ctx context.Context, filter string) error {
	msg, err := c.request(ctx, waitUnsubAck, 0, &protocol.Unsubscribe{TopicFilter: filter})
	if err != nil {
		return err
	}
	if status := msg.(*protocol.UnsubAck).Status; status != protocol.StatusOK {
		return fmt.Errorf("%w: UNSUBACK %s", ErrRejected, status)
	}
	c.mu.Lock()
	delete(c.subs, filter)
	c.mu.Unlock()
	return nil
}

// Close encerra a conexão, falha as operações pendentes com ErrClosed e espera
// as goroutines internas terminarem.
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	c.wg.Wait()
	return nil
}

func (c *Client) request(ctx context.Context, kind waiterKind, seq uint64, msg protocol.Message) (protocol.Message, error) {
	for {
		conn, err := c.waitConnected(ctx)
		if err != nil {
			return nil, err
		}
		response, err := c.requestOnConn(ctx, conn, kind, seq, msg)
		if errors.Is(err, errStaleConnection) {
			continue
		}
		return response, err
	}
}

func (c *Client) requestOnConn(ctx context.Context, conn net.Conn, kind waiterKind, seq uint64, msg protocol.Message) (protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w := waiter{ch: make(chan response, 1), seq: seq}

	c.writeMu.Lock()
	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, err
	}
	if c.conn != conn {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, errStaleConnection
	}
	c.pending[kind] = append(c.pending[kind], w)
	c.mu.Unlock()
	err := c.writeLocked(ctx, conn, msg)
	c.writeMu.Unlock()

	if err != nil {
		// Uma escrita interrompida pode deixar o fluxo com meio quadro.
		c.connectionLost(conn, fmt.Errorf("envio de %s: %w", msg.Type(), err))
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if term := c.Err(); term != nil {
			return nil, term
		}
		return nil, fmt.Errorf("%w: envio de %s: %w", ErrDisconnected, msg.Type(), err)
	}

	select {
	case r := <-w.ch:
		return r.msg, r.err
	case <-ctx.Done():
		// O waiter continua na fila e absorve a resposta quando ela chegar,
		// mantendo a correlação das próximas respostas.
		return nil, ctx.Err()
	}
}

func (c *Client) writeLocked(ctx context.Context, conn net.Conn, msg protocol.Message) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(deadline)
		defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	}
	return protocol.WriteMessage(conn, msg)
}

// writeCurrent envia uma mensagem sem waiter (DELIVER_ACK) na conexão ativa.
func (c *Client) writeCurrent(msg protocol.Message) (net.Conn, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.Lock()
	conn := c.conn
	term := c.terminal
	c.mu.Unlock()

	if term != nil {
		return nil, term
	}
	if conn == nil {
		return nil, ErrDisconnected
	}
	return conn, protocol.WriteMessage(conn, msg)
}

func (c *Client) route(msg protocol.Message) error {
	switch m := msg.(type) {
	case *protocol.PubAck:
		return c.resolve(waitPubAck, m, m.Seq)
	case *protocol.SubAck:
		return c.resolve(waitSubAck, m, 0)
	case *protocol.UnsubAck:
		return c.resolve(waitUnsubAck, m, 0)
	case *protocol.Deliver:
		c.mu.Lock()
		c.inbox = append(c.inbox, Message{
			Topic:       m.Topic,
			SensorID:    m.SensorID,
			Seq:         m.Seq,
			Timestamp:   time.UnixMilli(m.TimestampMS).UTC(),
			Temperature: m.Temperature,
			Gap:         m.Flags&protocol.DeliverFlagGap != 0,
			Status:      m.Status,
		})
		c.mu.Unlock()
		select {
		case c.inboxSignal <- struct{}{}:
		default:
		}
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrUnexpectedMessage, msg.Type())
	}
}

// resolve entrega a resposta ao pedido mais antigo do tipo. Para PUBACK, o seq
// confirma que a correlação por ordem não se perdeu.
func (c *Client) resolve(kind waiterKind, msg protocol.Message, seq uint64) error {
	c.mu.Lock()
	if len(c.pending[kind]) == 0 {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s sem pedido pendente", ErrUnexpectedMessage, msg.Type())
	}
	w := c.pending[kind][0]
	if w.seq != seq {
		c.mu.Unlock()
		return fmt.Errorf("%w: %s seq %d, esperado %d", ErrUnexpectedMessage, msg.Type(), seq, w.seq)
	}
	c.pending[kind][0] = waiter{}
	c.pending[kind] = c.pending[kind][1:]
	c.mu.Unlock()
	w.ch <- response{msg: msg}
	return nil
}

// deliverLoop repassa os DELIVERs para a aplicação fora da goroutine de
// leitura, para que um consumidor lento não atrase PUBACK, SUBACK e UNSUBACK.
func (c *Client) deliverLoop() {
	defer c.wg.Done()
	defer close(c.messages)
	for {
		c.mu.Lock()
		var next *Message
		if len(c.inbox) > 0 {
			m := c.inbox[0]
			next = &m
			c.inbox[0] = Message{}
			c.inbox = c.inbox[1:]
		}
		c.mu.Unlock()

		if next == nil {
			select {
			case <-c.inboxSignal:
				continue
			case <-c.done:
				return
			}
		}

		select {
		case c.messages <- *next:
		case <-c.done:
			return
		}

		ack := &protocol.DeliverAck{Topic: next.Topic, SensorID: next.SensorID, Seq: next.Seq}
		conn, err := c.writeCurrent(ack)
		if err != nil {
			if errors.Is(err, ErrDisconnected) {
				if conn != nil {
					c.connectionLost(conn, fmt.Errorf("envio de DELIVER_ACK: %w", err))
				}
				// O broker reenvia a leitura sem ACK após a reconexão.
				continue
			}
			return
		}
	}
}
