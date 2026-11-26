# Estudo — funções principais do João

Módulos: **broker**, **replication**, **juiz** e **vip**.

Cada função tem o código real (às vezes com trechos omitidos em `// ...`), o
que ela faz, por que foi feita assim, o que custa e uma pergunta provável da
defesa. Leia na ordem: ela segue o caminho de uma leitura.

```
sensor ─► vip ─► broker: sessão ─► fila justa ─► Publish ─► store ─► replicação ─► PUBACK
                                                               └─► fila do assinante ─► DELIVER ─► DELIVER_ACK ─► cursor
juiz: heartbeat ─► detecta queda ─► escolhe candidata ─► PROMOTE ─► reserva vira primário
```

---

## Parte 1 — Broker: entrada da leitura

### 1. `handlePublish` — fila do sensor e backpressure

[internal/broker/session.go:196](../internal/broker/session.go#L196)

**O que faz:** recebe um PUBLISH lido do socket, pega uma vaga na fila do
sensor, cria a comanda (`job`) e entrega ao `ackLoop` e ao escalonador.

```go
func (s *session) handlePublish(_ *slog.Logger, m *protocol.Publish) error {
	received := time.Now()
	if err := validatePublish(m); err != nil {
		return err
	}

	select {                       // a sessão já fechou?
	case <-s.closed:
		return errSessionClosed
	default:
	}

	select {
	case s.queue.slots <- struct{}{}:      // tem vaga: ocupa
	default:                               // não tem vaga
		s.server.scheduler.waits.Add(1)    // conta na métrica
		select {
		case s.queue.slots <- struct{}{}:  // ESPERA uma vaga liberar
		case <-s.closed:
			return errSessionClosed
		}
	}

	job := &publishJob{msg: m, done: make(chan publishOutcome, 1), received: received}
	select {
	case s.acks <- job:                    // garçom: responde em ordem
	case <-s.closed:
		return errSessionClosed
	}
	s.server.scheduler.enqueue(s.queue, job) // cozinha: processa por justiça
	return nil
}
```

- **Por quê:** `slots` é um canal de capacidade 32 (um semáforo). Sem vaga, esta
  goroutine, que é a que lê o socket, para. O buffer do TCP enche e o próprio
  TCP segura o sensor. É backpressure **por sensor**: os outros seguem normais.
- **Custo:** o sensor que lota a fila fica mais lento; ele não recebe erro, só
  espera.
- **Pergunta provável:** *"Por que não responder TEMPORARY_FAILURE quando a
  fila enche?"* — Porque o sensor reenviaria em seguida e geraria mais tráfego.
  Parando de ler, o TCP regula o ritmo sozinho, sem mensagem extra.

### 2. `ackLoop` — PUBACK em ordem, e só depois libera a vaga

[internal/broker/session.go:232](../internal/broker/session.go#L232)

**O que faz:** pega as comandas na ordem em que chegaram, espera o resultado
da gravação em `job.done`, manda o PUBACK e libera a vaga.

```go
func (s *session) ackLoop() {
	for {
		var job *publishJob
		select {
		case job = <-s.acks:          // próxima comanda, na ordem de chegada
		case <-s.closed:
			return
		}

		var outcome publishOutcome
		select {
		case outcome = <-job.done:    // espera a cozinha terminar
		case <-s.closed:
			return
		}

		if outcome.err != nil {       // falhou gravar: fecha SEM PUBACK
			// ... log
			_ = s.close()
			return
		}

		if err := s.write(&protocol.PubAck{Status: outcome.res.Status, Seq: job.msg.Seq}); err != nil {
			_ = s.close()
			return
		}
		<-s.queue.slots               // libera a vaga
		// ... métricas e log
	}
}
```

- **Por quê:** a vaga só é liberada **depois** de escrever o PUBACK. Assim, um
  sensor que não lê as respostas também fica limitado sozinho.
- **Erro na gravação:** fecha sem PUBACK. A leitura continua no buffer do
  sensor e é reenviada: falha de disco não vira perda.
- **Pergunta provável:** *"Por que separar a cozinha (escalonador) do garçom
  (ackLoop)?"* — A cozinha atende por justiça entre sensores; o garçom precisa
  responder cada sensor na ordem dele.

### 3. `enqueue` — a etiqueta da fila justa

[internal/broker/scheduler.go:127](../internal/broker/scheduler.go#L127)

**O que faz:** dá a "senha" da leitura e põe na fila do sensor.

```go
func (s *scheduler) enqueue(q *sensorQueue, job *publishJob) {
	s.mu.Lock()
	job.start = max(s.vtime, q.finish) // senha = maior entre "agora" e a próxima deste sensor
	q.finish = job.start + 1           // cada leitura custa 1
	job.arrival = s.arrivals           // desempate por ordem de chegada
	s.arrivals++
	q.jobs = append(q.jobs, job)
	if !q.queued {                     // sensor entra na lista de quem tem trabalho
		q.queued = true
		s.ready = append(s.ready, q)
	}
	s.mu.Unlock()
	s.cond.Signal()                    // acorda o run se ele dormia
}
```

- **Por quê:** sensor com backlog já tem senhas lá na frente; sensor em dia
  recebe a senha de agora e **passa à frente**. O `max` impede que um sensor
  parado acumule crédito.
- **Custo:** sem preempção, o sensor em dia ainda espera a gravação que já
  começou.
- **Pergunta provável:** *"Por que não uma cota fixa por sensor?"* — A cota
  atrasaria à toa o sensor que acabou de reconectar e precisa esvaziar o buffer,
  mesmo com o broker livre.

### 4. `run` + `nextLocked` — sempre a menor senha

[internal/broker/scheduler.go:78](../internal/broker/scheduler.go#L78)

```go
func (s *scheduler) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		for len(s.ready) == 0 && !s.closed {
			s.cond.Wait()                  // dorme sem trabalho
		}
		if len(s.ready) == 0 {
			s.mu.Unlock()
			return
		}

		i := s.nextLocked()               // sensor com a menor senha
		q := s.ready[i]
		job := q.jobs[0]
		q.jobs[0] = nil
		q.jobs = q.jobs[1:]
		if len(q.jobs) == 0 {             // esvaziou: sai da lista
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			q.queued = false
		}
		s.vtime = job.start               // o relógio virtual anda
		s.mu.Unlock()

		res, err := s.process(job.msg)    // grava e replica (Publish)
		job.done <- publishOutcome{res: res, err: err} // avisa o ackLoop
	}
}

func (s *scheduler) nextLocked() int {
	best := 0
	for i := 1; i < len(s.ready); i++ {
		a, b := s.ready[i].jobs[0], s.ready[best].jobs[0]
		if a.start < b.start || (a.start == b.start && a.arrival < b.arrival) {
			best = i
		}
	}
	return best
}
```

- **Por quê:** um único trabalhador processa uma leitura por vez. Isso mantém a
  ordem de gravação no store igual à ordem replicada.
- **Exemplo:** sensor R acorda com 1.000 leituras (senhas 0 a 999). Sensor N
  chega com senha igual ao `vtime` atual e sai logo depois da leitura em
  andamento.
- **Pergunta provável:** *"A busca é linear, isso não é lento?"* — `ready` só
  tem sensores com leitura pendente, não todos os conectados; para o cenário é
  pequeno.

### 5. `Publish` — reenvio, sem inscritos, gravação

[internal/broker/publish.go:101](../internal/broker/publish.go#L101)

```go
func (p *publisher) Publish(m *protocol.Publish) (PublishResult, error) {
	if err := validatePublish(m); err != nil {
		return PublishResult{}, err
	}

	st := p.sensor(m.SensorID)
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.seen && m.Seq <= st.lastSeq {   // reenvio de algo já gravado
		return PublishResult{Status: protocol.StatusPersisted, Stale: true}, nil
	}

	subscribers := p.topics.Subscribers(m.Topic)
	if len(subscribers) == 0 {            // ninguém inscrito: descarta
		return PublishResult{Status: protocol.StatusNoSubscribers}, nil
	}

	d := &protocol.Deliver{
		Topic:       m.Topic,
		SensorID:    m.SensorID,
		Seq:         m.Seq,
		TimestampMS: p.now().UnixMilli() - int64(m.AgeMS), // instante = agora - idade
		Temperature: m.Temperature,
	}
	if err := p.store.publish(d, subscribers); err != nil { // fsync + replicação
		return PublishResult{}, err
	}
	for _, clientID := range subscribers {  // uma cópia por assinante
		cp := *d
		if _, err := p.deliveries.Enqueue(clientID, &cp); err != nil {
			return PublishResult{}, err
		}
	}
	st.seen = true
	st.lastSeq = m.Seq

	return PublishResult{Status: protocol.StatusPersisted, Subscribers: len(subscribers)}, nil
}
```

- **Reenvio (`Stale`):** depois de um failover o sensor reenvia o que não teve
  PUBACK. Se já está gravado, o broker confirma **sem duplicar**.
- **`NO_SUBSCRIBERS`:** não há replay para quem se inscrever depois, então
  guardar seria inútil. O sensor tira a leitura do buffer.
- **`agora - age_ms`:** o sensor manda a idade da leitura, não o relógio dele.
  Não precisa sincronizar relógios.
- **Pergunta provável:** *"Inscrição de consumidor desconectado conta?"* —
  Conta. A inscrição é durável (fica no store); as leituras esperam na fila
  dele.

---

## Parte 2 — Broker: disco e saída

### 6. `syncRecord` + `appendRecord` — o cursor sem fsync

[internal/broker/store.go:319](../internal/broker/store.go#L319)

```go
// O DELIVER_ACK não exige fsync: perdê-lo numa queda da máquina só faz o
// broker reentregar leituras já entregues, que o consumidor descarta.
func syncRecord(kind storeRecordKind) bool {
	return kind != recAck
}

func appendRecord(log *storage.RecordLog, kind storeRecordKind, raw []byte) error {
	if syncRecord(kind) {
		return log.Append(raw)         // com fsync
	}
	return log.AppendBuffered(raw)     // sem fsync (só o cursor)
}
```

- **Por quê (Etapa 3):** cada leitura fazia 4 fsyncs em série (leitura e
  cursor, no primário e na reserva), de ~10 ms cada. O teto era ~22 leituras/s
  e o p95 chegou a 9,66 s. Sem fsync no cursor: 2 fsyncs, teto ~43/s, p95 de
  0,082 s.
- **Por que é seguro:**
  - queda do **processo**: o dado já está no cache do SO, que grava mesmo assim;
  - queda do **primário**: o cursor é replicado na reserva;
  - queda de **energia**: o cursor volta um pouco, o broker reentrega e o
    consumidor descarta a duplicata.
- **Regra de ouro:** perder o cursor causa reenvio; perder a leitura causa
  perda. Por isso só o cursor ficou sem fsync.

### 7. `push` — fila do assinante, retenção e `GAP_BEFORE`

[internal/broker/delivery.go:75](../internal/broker/delivery.go#L75)

```go
func (q *subscriberQueue) push(d *protocol.Deliver) (bool, []*protocol.Deliver) {
	q.mu.Lock()
	if q.cursor.covers(streamKey(d), d.Seq) { // já confirmada antes: ignora
		q.mu.Unlock()
		return false, nil
	}
	var dropped []*protocol.Deliver
	for q.q.len() >= q.limit && q.q.len() > 1 { // fila cheia (10.000)
		old := q.q.dropAt(1)                    // descarta a mais antiga DEPOIS da cabeça e já marca GAP_BEFORE na próxima do sensor
		delete(q.enqueued, keyOf(old))
		dropped = append(dropped, old)          // devolve a lista: Enqueue grava cada descarte no store
	}
	q.q.push(d)
	q.enqueued[keyOf(d)] = time.Now()
	q.drops += uint64(len(dropped))
	q.mu.Unlock()
	q.signal()
	return true, dropped
}
```

- **Por quê uma fila por assinante:** como no MQTT, o tópico só roteia. Cada
  consumidor tem fila, cursor e ritmo próprios; um lento não segura os outros.
- **Por que nunca a cabeça (índice 0):** ela pode estar em trânsito,
  esperando o DELIVER_ACK.
- **Custo:** consumidor ausente por muito tempo perde as antigas, mas **sabe**
  disso pelo `GAP_BEFORE`.

### 8. `ack` — o cursor avança

[internal/broker/delivery.go:106](../internal/broker/delivery.go#L106)

```go
func (q *subscriberQueue) ack(a *protocol.DeliverAck, persist func(stream string) error) (bool, error) {
	q.mu.Lock()
	if q.q.len() == 0 {
		q.mu.Unlock()
		return false, nil
	}
	h := q.q.items[0]
	if h.Topic != a.Topic || h.SensorID != a.SensorID || h.Seq != a.Seq {
		q.mu.Unlock()
		return false, nil                // ACK de algo que não é a cabeça: ignora
	}
	stream := streamKey(h)
	if err := persist(stream); err != nil { // grava o cursor (sem fsync) e replica
		q.mu.Unlock()
		return false, err                // falhou: a cabeça fica e será reenviada
	}
	q.cursor.advance(stream, h.Seq)
	q.q.items[0] = nil
	q.q.items = q.q.items[1:]            // só agora a cabeça sai
	q.mu.Unlock()
	q.signal()                           // libera o envio da próxima
	return true, nil
}
```

- **Stop-and-wait:** um DELIVER em voo por assinante. A cabeça só sai com o
  ACK dela. Garante a ordem por sensor e o reenvio do ponto certo após
  reconexão.
- **Custo:** a vazão de entrega por assinante depende da ida e volta.

---

## Parte 3 — Replicação

### 9. `Replicate` — o primário espera a reserva

[internal/replication/primary.go:160](../internal/replication/primary.go#L160)

```go
// Deve ser chamado com o lock de escrita do store, o que mantém na reserva a
// mesma ordem de registros do primário.
func (p *Primary) Replicate(record []byte) error {
	p.mu.Lock()
	index := p.index.Add(1)
	if p.conn != nil {
		msg := &protocol.Replica{Kind: protocol.ReplicaRecord, Index: index, Record: record}
		err := p.roundTripLocked(msg)   // envia e espera o REPLICA_ACK (timeout 2 s)
		if err == nil {
			p.mu.Unlock()
			return nil
		}
		p.desyncLocked(err)             // reserva caiu ou demorou: dessincronizada
	}
	p.mu.Unlock()

	if p.cfg.ConfirmAlone != nil {
		return p.cfg.ConfirmAlone(index) // sem reserva: precisa do aval do Juiz
	}
	return nil
}
```

- **Por quê:** o PUBACK só sai depois do REPLICA_ACK. Se o primário morrer com
  `kill -9`, toda leitura confirmada já está na reserva: **RPO = 0**.
- **O que é replicado:** cada registro do store (leituras, inscrições,
  cursores), não as mensagens dos clientes.
- **Custo:** fsync nos dois brokers mais a ida e volta em cada leitura; teto
  de ~43 leituras/s.
- **Pergunta provável:** *"E se a reserva cair?"* — O primário a marca como
  dessincronizada e segue sozinho, mas só confirma com o aval do Juiz
  (`confirmAlone`, abaixo). Quando ela volta, recebe a sincronização completa.

### 10. `Replica.handle` — a reserva aplica e responde (trecho)

[internal/replication/replica.go:108](../internal/replication/replica.go#L108)

```go
	for {
		msg, err := protocol.ReadMessage(reader)
		// ...
		switch m.Kind {
		case protocol.ReplicaSyncStart:          // começou sincronização completa
			syncing, pending = true, nil
			r.synced.Store(false)
			continue
		case protocol.ReplicaRecord:
			if syncing {                         // durante a sync, só acumula
				pending = append(pending, m.Record)
				continue
			}
		}

		status := protocol.StatusOK
		switch m.Kind {
		case protocol.ReplicaRecord:
			if err := r.target.Apply(m.Record); err != nil { // grava com fsync
				status = protocol.StatusTemporaryFailure
				r.synced.Store(false)
			} else {
				r.applied.Store(m.Index)
			}
		case protocol.ReplicaSyncEnd:            // fim da sync: troca o estado inteiro
			// ...
			if err := r.target.Reset(pending); err != nil { /* ... */ } else {
				r.applied.Store(m.Index)
				r.synced.Store(true)             // agora pode ser promovida
			}
			syncing, pending = false, nil
		}

		ack := &protocol.ReplicaAck{Status: status, Kind: m.Kind, Index: m.Index}
		if err := protocol.WriteMessage(conn, ack); err != nil {
			return
		}
	}
```

- **Por quê:** a reserva só se declara sincronizada depois de receber **todo**
  o estado. Antes disso, o Juiz não a promove.

### 11. `confirmAlone` — sem reserva, só com o aval do Juiz

[internal/broker/judge.go:206](../internal/broker/judge.go#L206)

```go
// Espera o Juiz ecoar um HEARTBEAT com índice >= index: a partir daí o Juiz
// sabe que o primário está à frente da reserva e não a promove (RPO = 0).
// Sem eco no prazo, o primário se isola.
func (l *judgeLink) confirmAlone(index uint64) error {
	if l.s.fenced.Load() {
		return errFenced
	}
	timer := time.NewTimer(l.fence)        // prazo de isolamento: 3 s
	defer timer.Stop()
	for l.acked.Load() < index {
		ch := l.ackChan()
		if l.acked.Load() >= index {
			break
		}
		l.notify()                         // manda HEARTBEAT agora
		select {
		case <-ch:                         // Juiz ecoou
		case <-timer.C:                    // Juiz não respondeu: se isola
			l.s.fence("reserva dessincronizada e juiz sem resposta")
			return errFenced
		case <-l.stop:
			return errFenced
		}
	}
	return nil
}
```

- **Por quê:** sem reserva sincronizada, se o primário confirmasse sozinho e
  depois morresse, o Juiz poderia promover uma reserva atrasada e perder
  leituras. Com o eco, o Juiz **sabe** que o primário está à frente e não
  promove ninguém atrasado.
- **Isolamento (fence):** sem eco, o primário para de atender. Os sensores
  guardam no buffer. Isso também impede dois primários (split-brain).

---

## Parte 4 — Juiz

### 12. `step` — a regra do failover (trecho)

[internal/juiz/decide.go:87](../internal/juiz/decide.go#L87)

```go
func (c *coordinator) step(now time.Time, views map[string]brokerView) result {
	var res result
	// Período de aprendizado: o Juiz recém-iniciado não decide antes de
	// ouvir os brokers por um prazo de falha completo.
	if now.Before(c.startedAt.Add(c.failureTimeout)) {
		return res
	}
	// ... adota o primário ativo

	active := views[c.active]
	activeUp := c.fresh(active, now)          // heartbeat nos últimos 5 s?

	// outro broker se dizendo primário? manda PROMOTE(ativo) para ele se rebaixar
	for _, id := range ids {
		v := views[id]
		if id != c.active && id != c.promoting && c.fresh(v, now) && isPrimaryLike(v.state) {
			res.actions = append(res.actions, action{to: id, primary: c.active})
		}
	}

	if activeUp {                             // primário vivo: nada a fazer
		// ...
		return res
	}

	// primário mudo: failover
	candidate, reason := c.candidate(now, views, ids, active)
	if candidate == "" {                      // ninguém apto: failover bloqueado
		// ... evento com o motivo
		return res
	}
	c.promoting, c.blocked = candidate, ""
	res.actions = append(res.actions, action{to: candidate, primary: candidate}) // PROMOTE
	return res
}
```

- **Por quê sem rede:** `decide.go` recebe o instante e as visões e devolve as
  ações. Não faz I/O, então todos os casos de failover são testados sem subir
  processos.
- **Período de aprendizado:** um Juiz que acabou de subir não promove ninguém
  antes de ouvir todos por 5 s. Evita promover por engano após reiniciar.

### 13. `candidate` — só promove quem não perdeu nada

[internal/juiz/decide.go:175](../internal/juiz/decide.go#L175)

```go
func (c *coordinator) candidate(now time.Time, views map[string]brokerView, ids []string, active brokerView) (string, string) {
	reason := "nenhuma reserva com heartbeat recente"
	for _, id := range ids {
		v := views[id]
		if id == c.active || !c.fresh(v, now) {
			continue                          // é o primário ou está mudo
		}
		switch {
		case v.state == protocol.BrokerStateBackupSynced && v.index >= active.index:
			return id, ""                     // pode promover
		case v.state == protocol.BrokerStateBackupSynced:
			reason = "reserva atrasada em relacao ao primario"
		case v.state == protocol.BrokerStateBackupSyncing:
			reason = "reserva dessincronizada"
		}
	}
	return "", reason
}
```

- **Por quê:** promover uma reserva atrasada perderia leituras confirmadas.
  Melhor ficar sem primário (os sensores guardam no buffer) do que perder dado.
- **Custo:** em casos raros o failover fica bloqueado até alguém voltar.

### 14. `shouldEcho` — o anti split-brain

[internal/juiz/decide.go:79](../internal/juiz/decide.go#L79)

```go
// Um primário que não é o ativo não recebe eco: sem eco, ele não consegue
// confirmar escritas sem reserva e se isola.
func (c *coordinator) shouldEcho(id string, state protocol.BrokerState) bool {
	if !isPrimaryLike(state) || c.active == "" {
		return true
	}
	return id == c.active || id == c.promoting
}
```

- **Cenário:** o b1 trava (não morre) por 6 s, o Juiz promove o b2, e o b1
  acorda achando que é primário. Ele não recebe eco do Juiz, não consegue
  confirmar escritas sozinho e se isola. Depois recebe PROMOTE(b2) e vira
  reserva.

### 15. `handlePromote` + `promote` — o lado do broker (trecho)

[internal/broker/judge.go:300](../internal/broker/judge.go#L300)

```go
func (s *Server) handlePromote(id string) {
	switch {
	case id == s.cfg.BrokerID && s.Role() == RoleBackup:
		s.promote()                          // sou a reserva escolhida
	case id == s.cfg.BrokerID:
		s.unfence("juiz confirmou este primario")
	case s.Role() == RolePrimary:
		s.demote(id)                         // outro foi escolhido: viro reserva
	default:
		// reserva ignora PROMOTE de outro
	}
}

func (s *Server) promote() {
	// ...
	if r := s.replica; r != nil {
		_, applied, connected := r.Status()
		if connected {                       // o primário ainda replica aqui: está vivo!
			s.log.Warn("PROMOTE recusado: o primario ainda replica nesta reserva")
			return
		}
		// ...
	}
	// monta tópicos, filas e cursores a partir do store replicado
	s.store.state.prune()
	state := s.store.recovered()
	s.resetMemory()
	s.loadState(state)
	// ... passa a replicar para o par quando ele voltar
	s.fenced.Store(false)
	s.role.Store(RolePrimary)                // agora aceita CONNECT
}
```

- **Segunda trava contra dois primários:** se a conexão de replicação ainda
  está aberta, o primário está vivo, e a reserva **recusa** a promoção.
- **`demote`:** o antigo primário que volta descarta o estado local e recebe a
  sincronização completa do novo primário.

---

## Parte 5 — vip

### 16. `handle` + `route` + `order` — um endereço só

[internal/vip/proxy.go:199](../internal/vip/proxy.go#L199)

```go
func (p *Proxy) handle(ctx context.Context, client net.Conn) {
	// ...
	msg, err := protocol.ReadMessage(cr)       // lê a 1ª mensagem do cliente
	connect, ok := msg.(*protocol.Connect)     // tem que ser CONNECT
	// ...
	backend, br, addr, err := p.route(connect)
	if err != nil {                            // nenhum broker aceitou
		_ = protocol.WriteMessage(client, &protocol.ConnAck{Status: protocol.StatusTemporaryFailure})
		return
	}
	// ...
	if err := protocol.WriteMessage(client, &protocol.ConnAck{Status: protocol.StatusOK}); err != nil {
		return
	}
	// ...
	pipe(client, cr, backend, br)              // daqui em diante só copia bytes
}

func (p *Proxy) route(connect *protocol.Connect) (net.Conn, *bufio.Reader, string, error) {
	last := errNoPrimary
	for _, addr := range p.order() {           // preferido primeiro
		conn, br, err := p.tryBackend(addr, connect) // manda o CONNECT; reserva recusa
		if err != nil {
			last = fmt.Errorf("%s: %w", addr, err)
			continue
		}
		p.preferred.Store(addr)                // lembra quem aceitou
		return conn, br, addr, nil
	}
	return nil, nil, "", last
}

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
```

- **Por quê:** os clientes só conhecem um endereço. O vip não precisa saber
  quem é o primário: **o próprio broker decide** aceitando ou recusando o
  CONNECT (a reserva responde `TEMPORARY_FAILURE` até ser promovida).
- **No failover:** a conexão TCP do cliente cai junto com o primário; a
  biblioteca reconecta no mesmo endereço e o vip acha quem aceita agora.
- **Custo:** o vip é um processo único no caminho dos dados (ressalva no
  `docs/ressalvas.md`). Ele simula um IP virtual, que em produção seria VRRP ou
  DNS.

---

## Autoteste (responda em voz alta, sem olhar)

1. O que acontece quando a fila de um sensor enche? Onde está no código?
2. Como a fila justa decide quem sai? Dê um exemplo com um sensor em rajada.
3. Por que o PUBACK só sai depois do REPLICA_ACK? O que custa?
4. Por que só o cursor ficou sem fsync? E se a máquina cair?
5. A fila do assinante encheu. O que é descartado e como o consumidor sabe?
6. A reserva caiu. O primário para? Como ele confirma escritas?
7. Quando o Juiz promove uma reserva? Quando ele se recusa?
8. Como o sistema evita dois primários? (São duas travas.)
9. Por que o vip não precisa saber quem é o primário?
10. O primário morreu depois de gravar, mas antes do PUBACK. A leitura se
    perde? Chega duplicada?
