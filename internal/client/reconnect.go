package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/JoaoPedro-MAAR/Maktuf/internal/protocol"
	"github.com/JoaoPedro-MAAR/Maktuf/internal/storage"
)

const (
	DefaultReconnectBaseDelay = 100 * time.Millisecond
	DefaultReconnectMaxDelay  = 30 * time.Second
	DefaultReconnectJitter    = 0.2
	DefaultReconnectDial      = 5 * time.Second
)

var errStaleConnection = errors.New("client: conexao mudou antes do envio")

// ReconnectOptions controla a reconexão automática (RF-53, RF-58).
//
// O valor zero do struct inteiro usa os defaults acima. Quando algum campo é
// informado, os campos de duração zerados recebem o default aplicável e
// Jitter zero significa sem jitter.
type ReconnectOptions struct {
	// BaseDelay é o primeiro intervalo antes de tentar reconectar.
	BaseDelay time.Duration
	// MaxDelay limita o crescimento do intervalo.
	MaxDelay time.Duration
	// Jitter é a fração de variação aleatória em torno do intervalo
	// calculado, entre 0 e 1.
	Jitter float64
	// DialTimeout limita cada tentativa de conexão e handshake.
	DialTimeout time.Duration
	// MaxAttempts limita as tentativas consecutivas. Zero significa tentar
	// indefinidamente com o intervalo limitado por MaxDelay.
	MaxAttempts int
}

func (o ReconnectOptions) normalized() ReconnectOptions {
	if o == (ReconnectOptions{}) {
		return ReconnectOptions{
			BaseDelay:   DefaultReconnectBaseDelay,
			MaxDelay:    DefaultReconnectMaxDelay,
			Jitter:      DefaultReconnectJitter,
			DialTimeout: DefaultReconnectDial,
		}
	}
	if o.BaseDelay <= 0 {
		o.BaseDelay = DefaultReconnectBaseDelay
	}
	if o.MaxDelay <= 0 {
		o.MaxDelay = DefaultReconnectMaxDelay
	}
	if o.MaxDelay < o.BaseDelay {
		o.MaxDelay = o.BaseDelay
	}
	if o.Jitter < 0 {
		o.Jitter = 0
	}
	if o.Jitter > 1 {
		o.Jitter = 1
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = DefaultReconnectDial
	}
	return o
}

func (o ReconnectOptions) delay(attempt int) time.Duration {
	d := o.BaseDelay
	for i := 0; i < attempt && d < o.MaxDelay; i++ {
		if d > o.MaxDelay/2 {
			d = o.MaxDelay
			break
		}
		d *= 2
	}
	if d > o.MaxDelay {
		d = o.MaxDelay
	}
	if o.Jitter > 0 {
		factor := 1 + o.Jitter*(2*rand.Float64()-1)
		d = time.Duration(float64(d) * factor)
	}
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// Options configura a biblioteca cliente.
type Options struct {
	Reconnect ReconnectOptions
	// Sensor habilita o buffer durável de um sensor: leituras pendentes são
	// reenviadas automaticamente na conexão inicial e após cada reconexão.
	Sensor *SensorOptions
}

// SensorOptions associa o buffer durável do sensor ao tópico de publicação.
type SensorOptions struct {
	Buffer storage.Buffer
	Topic  string
	// SensorID é o identificador enviado no PUBLISH. Vazio usa o clientID.
	SensorID string
}

type sensorBuffer struct {
	buffer   storage.Buffer
	topic    string
	sensorID string

	mu sync.Mutex
}

// ConnectWithOptions abre a conexão inicial e configura reconexão e buffer do
// sensor. Erros na conexão inicial são devolvidos imediatamente; depois disso a
// reconexão é automática.
func ConnectWithOptions(ctx context.Context, addr string, role Role, clientID string, opts Options) (*Client, error) {
	if clientID == "" {
		return nil, fmt.Errorf("%w: client_id vazio", ErrInvalidArgument)
	}
	if role != RoleSensor && role != RoleConsumer {
		return nil, fmt.Errorf("%w: papel %d", ErrInvalidArgument, role)
	}

	opts.Reconnect = opts.Reconnect.normalized()
	if opts.Sensor != nil {
		sensor := *opts.Sensor
		if role != RoleSensor {
			return nil, fmt.Errorf("%w: buffer do sensor exige papel de sensor", ErrInvalidArgument)
		}
		if sensor.Buffer == nil {
			return nil, fmt.Errorf("%w: buffer do sensor nulo", ErrInvalidArgument)
		}
		if !validTopic(sensor.Topic) {
			return nil, fmt.Errorf("%w: topico do sensor %q", ErrInvalidArgument, sensor.Topic)
		}
		if sensor.SensorID == "" {
			sensor.SensorID = clientID
		}
		opts.Sensor = &sensor
	}

	conn, reader, err := dialAndHandshake(ctx, addr, role, clientID, opts.Reconnect.DialTimeout)
	if err != nil {
		return nil, err
	}

	c := newClient(addr, role, clientID, opts)
	if err := c.adopt(conn, reader); err != nil {
		c.shutdown(err)
		c.wg.Wait()
		return nil, err
	}

	c.wg.Add(1)
	go c.reconnectLoop()
	if c.sensor != nil {
		c.wg.Add(1)
		go c.pendingLoop()
	}
	c.wg.Add(1)
	go c.deliverLoop()
	return c, nil
}

func dialAndHandshake(ctx context.Context, addr string, role Role, clientID string, timeout time.Duration) (net.Conn, *bufio.Reader, error) {
	dialCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var d net.Dialer
	conn, err := d.DialContext(dialCtx, "tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("client: conectar a %s: %w", addr, err)
	}

	reader := bufio.NewReader(conn)
	if err := handshake(dialCtx, conn, reader, role, clientID); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("%w: %s: %w", ErrHandshake, addr, err)
	}
	return conn, reader, nil
}

func (c *Client) startReadLoop(conn net.Conn, r *bufio.Reader) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.readLoop(conn, r)
	}()
}

func (c *Client) readLoop(conn net.Conn, r *bufio.Reader) {
	for {
		msg, err := protocol.ReadMessage(r)
		if err != nil {
			c.connectionLost(conn, fmt.Errorf("leitura do broker: %w", err))
			return
		}
		if err := c.route(msg); err != nil {
			c.connectionLost(conn, err)
			return
		}
	}
}

// notifyLocked acorda quem espera mudança de estado. O chamador mantém mu.
func (c *Client) notifyLocked() {
	close(c.stateChanged)
	c.stateChanged = make(chan struct{})
}

func (c *Client) connectionLost(conn net.Conn, cause error) {
	c.mu.Lock()
	if c.terminal != nil || c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.conn = nil
	c.restoring = false
	pending := c.pending
	c.pending = [waiterKinds][]waiter{}
	err := fmt.Errorf("%w: %v", ErrDisconnected, cause)
	c.notifyLocked()
	c.mu.Unlock()

	_ = conn.Close()
	for _, waiters := range pending {
		for _, w := range waiters {
			w.ch <- response{err: err}
		}
	}
}

func (c *Client) waitConnected(ctx context.Context) (net.Conn, error) {
	for {
		c.mu.Lock()
		if c.terminal != nil {
			err := c.terminal
			c.mu.Unlock()
			return nil, err
		}
		if c.conn != nil && !c.restoring {
			conn := c.conn
			c.mu.Unlock()
			return conn, nil
		}
		changed := c.stateChanged
		c.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, c.Err()
		}
	}
}

// waitDisconnected bloqueia até a conexão cair ou o cliente terminar. Devolve
// false quando o cliente não deve mais reconectar.
func (c *Client) waitDisconnected(ctx context.Context) bool {
	for {
		c.mu.Lock()
		if c.terminal != nil {
			c.mu.Unlock()
			return false
		}
		if c.conn == nil {
			c.mu.Unlock()
			return true
		}
		changed := c.stateChanged
		c.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return false
		case <-c.done:
			return false
		}
	}
}

func (c *Client) adopt(conn net.Conn, reader *bufio.Reader) error {
	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		_ = conn.Close()
		return err
	}
	c.conn = conn
	c.restoring = true
	c.notifyLocked()
	c.mu.Unlock()

	c.startReadLoop(conn, reader)

	ctx, cancel := context.WithTimeout(c.ctx, c.opts.Reconnect.DialTimeout)
	err := c.restoreSubscriptions(ctx, conn)
	cancel()
	if err != nil {
		c.connectionLost(conn, fmt.Errorf("restaurar inscricoes: %w", err))
		return err
	}

	c.mu.Lock()
	if c.conn == conn && c.terminal == nil {
		c.restoring = false
		c.notifyLocked()
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) restoreSubscriptions(ctx context.Context, conn net.Conn) error {
	c.mu.Lock()
	filters := make([]string, 0, len(c.subs))
	for filter := range c.subs {
		filters = append(filters, filter)
	}
	c.mu.Unlock()

	for _, filter := range filters {
		reply, err := c.requestOnConn(ctx, conn, waitSubAck, 0, &protocol.Subscribe{TopicFilter: filter})
		if err != nil {
			return err
		}
		if status := reply.(*protocol.SubAck).Status; status != protocol.StatusOK {
			return fmt.Errorf("%w: SUBACK %s ao restaurar %q", ErrRejected, status, filter)
		}
	}
	return nil
}

func (c *Client) reconnectLoop() {
	defer c.wg.Done()
	attempt := 0

	for {
		if !c.waitDisconnected(c.ctx) {
			return
		}
		for {
			if c.Err() != nil {
				return
			}
			if max := c.opts.Reconnect.MaxAttempts; max > 0 && attempt >= max {
				c.shutdown(ErrReconnectExhausted)
				return
			}

			timer := time.NewTimer(c.opts.Reconnect.delay(attempt))
			select {
			case <-timer.C:
			case <-c.done:
				timer.Stop()
				return
			}

			if c.Err() != nil {
				return
			}

			conn, reader, err := dialAndHandshake(c.ctx, c.addr, c.role, c.clientID, c.opts.Reconnect.DialTimeout)
			if err != nil {
				attempt++
				continue
			}
			if err := c.adopt(conn, reader); err != nil {
				attempt++
				continue
			}

			attempt = 0
			break
		}
	}
}

// shutdown encerra o cliente de forma definitiva, fechando Done e falhando as
// operações pendentes.
func (c *Client) shutdown(err error) {
	c.mu.Lock()
	if c.terminal != nil {
		c.mu.Unlock()
		return
	}
	c.terminal = err
	pending := c.pending
	c.pending = [waiterKinds][]waiter{}
	conn := c.conn
	c.conn = nil
	c.restoring = false
	close(c.done)
	c.notifyLocked()
	c.mu.Unlock()

	c.cancel()
	if conn != nil {
		_ = conn.Close()
	}
	for _, waiters := range pending {
		for _, w := range waiters {
			w.ch <- response{err: err}
		}
	}
}

// PublishBuffered registra a leitura no buffer durável e a envia em ordem
// de seq, confirmando-a no buffer somente após PERSISTED ou NO_SUBSCRIBERS. Se
// a conexão cair antes do ACK, a mesma leitura é reenviada após reconectar.
func (c *Client) PublishBuffered(ctx context.Context, measuredAt time.Time, temperature float32) (storage.Reading, Status, error) {
	if c.sensor == nil {
		return storage.Reading{}, 0, fmt.Errorf("%w: buffer do sensor nao configurado", ErrInvalidArgument)
	}

	s := c.sensor
	s.mu.Lock()
	defer s.mu.Unlock()

	reading, err := s.buffer.Append(measuredAt, temperature)
	if err != nil {
		return storage.Reading{}, 0, err
	}

	for {
		status, err := c.flushLocked(ctx, reading.Seq)
		if err == nil {
			return reading, status, nil
		}
		if errors.Is(err, ErrDisconnected) && ctx.Err() == nil && c.Err() == nil {
			continue
		}
		return reading, status, err
	}
}

// FlushPending reenvia todas as leituras pendentes do buffer do sensor.
func (c *Client) FlushPending(ctx context.Context) error {
	if c.sensor == nil {
		return fmt.Errorf("%w: buffer do sensor nao configurado", ErrInvalidArgument)
	}

	c.sensor.mu.Lock()
	defer c.sensor.mu.Unlock()

	for {
		_, err := c.flushLocked(ctx, 0)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrDisconnected) && ctx.Err() == nil && c.Err() == nil {
			continue
		}
		return err
	}
}

func (c *Client) flushLocked(ctx context.Context, target uint64) (Status, error) {
	var targetStatus Status
	targetFound := false

	for _, reading := range c.sensor.buffer.Pending() {
		status, err := c.publishBufferedReading(ctx, reading)
		if err != nil {
			return 0, err
		}
		if reading.Seq == target {
			targetStatus = status
			targetFound = true
		}
	}
	if target != 0 && !targetFound {
		// A leitura já saiu do buffer, logo foi confirmada em uma passagem
		// anterior. O buffer só remove em PERSISTED ou NO_SUBSCRIBERS.
		return StatusPersisted, nil
	}
	return targetStatus, nil
}

func (c *Client) publishBufferedReading(ctx context.Context, reading storage.Reading) (Status, error) {
	status, err := c.Publish(ctx, c.sensor.topic, Reading{
		SensorID:    c.sensor.sensorID,
		Seq:         reading.Seq,
		Age:         reading.Age(time.Now()),
		Temperature: reading.Temperature,
	})
	if err != nil {
		return 0, err
	}
	switch status {
	case StatusPersisted, StatusNoSubscribers:
		if _, err := c.sensor.buffer.Ack(reading.Seq); err != nil {
			return 0, err
		}
		return status, nil
	default:
		return status, fmt.Errorf("%w: status %s", ErrTemporaryFailure, status)
	}
}

// pendingLoop drena o buffer do sensor na conexão inicial e depois de cada
// reconexão, cobrindo inclusive reinício do processo.
func (c *Client) pendingLoop() {
	defer c.wg.Done()
	for {
		if _, err := c.waitConnected(c.ctx); err != nil {
			return
		}
		_ = c.FlushPending(c.ctx)
		if !c.waitDisconnected(c.ctx) {
			return
		}
	}
}

func validTopic(topic string) bool {
	if topic == "" {
		return false
	}
	for _, level := range strings.Split(topic, "/") {
		if level == "" || strings.Contains(level, "#") {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	return !strings.Contains(topic, "+")
}
