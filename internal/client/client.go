// Package client é a biblioteca de comunicação do Maktuf. Ela esconde o
// socket TCP e o enquadramento binário do protocolo (docs/protocolo.md) atrás
// das operações Connect, Publish, Subscribe, Unsubscribe e Close (RF-51).
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
//	// Messages é fechado quando a conexão termina; c.Err() informa a causa.
//
// Todos os métodos podem ser chamados concorrentemente por várias goroutines
// (RF-55). Reconexão automática ainda não é feita: quando a conexão cai, Done
// é fechado e as operações passam a falhar com ErrDisconnected.
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

	// ErrDisconnected indica que a conexão com o broker foi perdida.
	ErrDisconnected = errors.New("client: conexao com o broker perdida")

	// ErrHandshake indica que o broker recusou ou não concluiu o CONNECT.
	ErrHandshake = errors.New("client: handshake CONNECT/CONNACK falhou")

	// ErrRejected indica que o broker respondeu com status diferente de OK.
	ErrRejected = errors.New("client: operacao recusada pelo broker")

	// ErrUnexpectedMessage indica uma resposta fora do protocolo; a conexão é
	// encerrada porque o fluxo deixou de ser confiável.
	ErrUnexpectedMessage = errors.New("client: mensagem inesperada do broker")

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
// leitura é feita por uma única goroutine que roteia respostas e DELIVERs.
//
// Como SUBACK e UNSUBACK não identificam o pedido, as respostas são
// correlacionadas pela ordem: o broker trata as mensagens de uma sessão em
// sequência, e o waiter é enfileirado sob writeMu junto com a escrita.
type Client struct {
	conn     net.Conn
	clientID string

	writeMu sync.Mutex

	mu      sync.Mutex
	pending [waiterKinds][]waiter
	inbox   []Message
	err     error

	inboxSignal chan struct{}
	done        chan struct{}
	messages    chan Message
	wg          sync.WaitGroup
}

// Connect abre a conexão TCP, envia CONNECT e valida o CONNACK. O contexto
// limita o tempo de conexão e do handshake.
func Connect(ctx context.Context, addr string, role Role, clientID string) (*Client, error) {
	if clientID == "" {
		return nil, fmt.Errorf("%w: client_id vazio", ErrInvalidArgument)
	}
	if role != RoleSensor && role != RoleConsumer {
		return nil, fmt.Errorf("%w: papel %d", ErrInvalidArgument, role)
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: conectar a %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)
	if err := handshake(ctx, conn, reader, role, clientID); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("%w: %s: %w", ErrHandshake, addr, err)
	}

	c := &Client{
		conn:        conn,
		clientID:    clientID,
		inboxSignal: make(chan struct{}, 1),
		done:        make(chan struct{}),
		messages:    make(chan Message, messageBuffer),
	}
	c.wg.Add(2)
	go c.readLoop(reader)
	go c.deliverLoop()
	return c, nil
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
// a mesma leitura pode chegar mais de uma vez. O canal é fechado quando a
// conexão termina.
func (c *Client) Messages() <-chan Message { return c.messages }

// Done é fechado quando a conexão termina, por Close ou por falha.
func (c *Client) Done() <-chan struct{} { return c.done }

// Err devolve a causa do encerramento, ou nil enquanto a conexão está ativa.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
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
	if err := ctx.Err(); err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.Err(); err != nil {
		return err
	}
	return c.writeLocked(ctx, &protocol.Sleep{DurationMS: uint32(duration.Milliseconds())})
}

// Subscribe registra o filtro e aguarda o SUBACK. Inscrições sobrevivem à
// desconexão no broker e só são removidas por Unsubscribe.
func (c *Client) Subscribe(ctx context.Context, filter string) error {
	msg, err := c.request(ctx, waitSubAck, 0, &protocol.Subscribe{TopicFilter: filter})
	if err != nil {
		return err
	}
	if status := msg.(*protocol.SubAck).Status; status != protocol.StatusOK {
		return fmt.Errorf("%w: SUBACK %s", ErrRejected, status)
	}
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
	return nil
}

// Close encerra a conexão, falha as operações pendentes com ErrClosed e espera
// as goroutines internas terminarem.
func (c *Client) Close() error {
	c.fail(ErrClosed)
	c.wg.Wait()
	return nil
}

func (c *Client) request(ctx context.Context, kind waiterKind, seq uint64, msg protocol.Message) (protocol.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	w := waiter{ch: make(chan response, 1), seq: seq}

	c.writeMu.Lock()
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		c.writeMu.Unlock()
		return nil, err
	}
	c.pending[kind] = append(c.pending[kind], w)
	c.mu.Unlock()
	err := c.writeLocked(ctx, msg)
	c.writeMu.Unlock()

	if err != nil {
		// Uma escrita interrompida pode deixar o fluxo com meio quadro.
		c.fail(fmt.Errorf("%w: envio de %s: %w", ErrDisconnected, msg.Type(), err))
		return nil, ctxErr(ctx, c.Err())
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

func (c *Client) writeLocked(ctx context.Context, msg protocol.Message) error {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
		defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	}
	return protocol.WriteMessage(c.conn, msg)
}

func (c *Client) write(msg protocol.Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return protocol.WriteMessage(c.conn, msg)
}

func (c *Client) readLoop(r *bufio.Reader) {
	defer c.wg.Done()
	for {
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			c.fail(fmt.Errorf("%w: %w", ErrDisconnected, err))
			return
		}
		if err := c.route(msg); err != nil {
			c.fail(err)
			return
		}
	}
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
		if err := c.write(ack); err != nil {
			c.fail(fmt.Errorf("%w: envio de DELIVER_ACK: %w", ErrDisconnected, err))
			return
		}
	}
}

func (c *Client) fail(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = err
	pending := c.pending
	c.pending = [waiterKinds][]waiter{}
	close(c.done)
	c.mu.Unlock()

	_ = c.conn.Close()
	for _, waiters := range pending {
		for _, w := range waiters {
			w.ch <- response{err: err}
		}
	}
}
