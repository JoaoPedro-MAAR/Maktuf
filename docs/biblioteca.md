# Biblioteca cliente

A biblioteca `internal/client` é como sensores e consumidores falam com o
Maktuf (RF-51 a RF-58). Ela esconde:
- o socket TCP e o protocolo binário ([protocolo.md](protocolo.md));
- a reconexão com backoff e a restauração das inscrições;
- o reenvio do buffer durável do sensor;
- o failover: o cliente usa um único endereço, o do vip, e não sabe qual
  broker é o primário (RF-44, RF-49).

A referência completa está no godoc:

```bash
go doc -all ./internal/client
```

Os exemplos abaixo também existem como funções `Example` em
`internal/client/tests/example_test.go`, compiladas pelo `go test`; assim, não
ficam desatualizados em relação ao código.

## Conceitos

| Conceito | O que é |
|---|---|
| Papel | `client.RoleSensor` publica; `client.RoleConsumer` se inscreve e recebe |
| `clientID` | Identificador no `CONNECT`. Para o consumidor, é ele que guarda o cursor de entrega no broker: reconectar com o mesmo id retoma de onde parou |
| Tópico | `sensores/{id}/temperatura` para leituras e `sensores/{id}/status` para o estado do sensor. Filtros aceitam `+` para um nível (`sensores/+/temperatura`) |
| Garantia | **Pelo menos uma vez**: uma leitura pode chegar repetida, então deduplique por `sensor_id + seq` (RF-14) |
| Ordem | Por sensor: o `seq` de um mesmo sensor chega em ordem crescente (RF-15) |
| Instante | O sensor envia a idade da leitura (`age`); o broker calcula `Timestamp = recebimento − idade` (DP-10) |

## Consumidor

```go
c, err := client.Connect(ctx, "127.0.0.1:7070", client.RoleConsumer, "consumidor-1")
if err != nil {
	return err
}
defer c.Close()

if err := c.Subscribe(ctx, "sensores/+/temperatura"); err != nil {
	return err
}
_ = c.Subscribe(ctx, "sensores/+/status")

dedup := client.NewDedup(0)
for m := range c.Messages() {
	if !dedup.AcceptReading(m) {
		continue // repetida
	}
	switch {
	case m.Status != client.SensorStateUnknown:
		// transição online / dormindo / offline (RF-21)
	case m.Gap:
		// o broker descartou leituras deste sensor por retenção antes desta (RF-37)
	}
	fmt.Println(m.SensorID, m.Seq, m.Temperature, m.Timestamp)
}
```

- **`Messages()`** só fecha no encerramento final (`Close`, ou reconexão
  esgotada com `MaxAttempts`). Quedas transitórias não fecham o canal.
- **O `DELIVER_ACK` sai quando a mensagem entra no canal**, que tem 64
  posições, e não quando a aplicação termina de processá-la. Uma aplicação que
  cair com mensagens no canal não as recebe de novo (ver
  [ressalvas](ressalvas.md)).
- **`Message.Gap`** indica lacuna sinalizada pelo broker (retenção). Um salto de
  `seq` **sem** `Gap` vem de antes do broker: overflow do buffer do sensor.
- **`Unsubscribe(ctx, filtro)`** para entregas futuras. O que já estava na fila
  ainda chega (RF-10).

## Sensor

### Simples, sem buffer

```go
c, err := client.Connect(ctx, "127.0.0.1:7070", client.RoleSensor, "sensor-a")
status, err := c.Publish(ctx, "sensores/sensor-a/temperatura", client.Reading{
	SensorID: "sensor-a", Seq: 1, Age: 2 * time.Second, Temperature: 23.5,
})
```

`status` é `client.StatusPersisted` (gravado e, com reserva sincronizada,
replicado) ou `client.StatusNoSubscribers` (descartado por falta de
inscritos, RF-07). Nesse modo, a aplicação cuida do `seq` e do reenvio.

### Com buffer durável (recomendado)

```go
buffer, err := storage.OpenSensorBuffer("data/sensor-a.buf", storage.BufferOptions{
	MaxReadings: 86400, // 24 h a 1 leitura/s; ao encher, descarta a mais antiga (RF-26)
	SensorID:    "sensor-a",
})
defer buffer.Close()

c, err := client.ConnectWithOptions(ctx, "127.0.0.1:7070", client.RoleSensor, "sensor-a", client.Options{
	Sensor: &client.SensorOptions{Buffer: buffer, Topic: "sensores/sensor-a/temperatura"},
})
defer c.Close()

reading, status, err := c.PublishBuffered(ctx, time.Now(), 23.5)
```

Com o buffer configurado:
- **`PublishBuffered`** grava a leitura no disco com o próximo `seq`, envia
  tudo que está pendente em ordem e só remove do buffer com `PERSISTED` ou
  `NO_SUBSCRIBERS` (RF-23 a RF-25).
- **Se der erro ou estourar o contexto**, a leitura **continua no buffer** e
  sai antes da próxima.
- **Na conexão inicial e em cada reconexão**, a biblioteca reenvia o pendente
  sozinha, inclusive depois de reiniciar o processo. `FlushPending(ctx)`
  força o envio.
- **Uma leitura por vez em voo:** a próxima só sai depois do `PUBACK` da
  anterior. Rajadas ficam no buffer (RF-35).
- **`Sleep(ctx, duração)`** avisa o broker antes de desligar. O tópico de
  status recebe `dormindo` e, se o sensor não voltar em `duração +
  tolerância`, `offline` (RF-27, RF-28).

## Reconexão e failover

```go
client.Options{
	Reconnect: client.ReconnectOptions{
		BaseDelay:   100 * time.Millisecond, // primeira espera
		MaxDelay:    30 * time.Second,       // teto do backoff (dobra a cada falha)
		Jitter:      0.2,                    // ±20%
		DialTimeout: 5 * time.Second,        // cada tentativa
		MaxAttempts: 0,                      // 0 = para sempre
	},
}
```

- **Durante um failover**, o vip recusa o `CONNECT` até a reserva ser
  promovida, e o cliente tenta de novo com backoff. Ao conectar, restaura as
  inscrições e reenvia o buffer, sem nada da aplicação.
- **O tempo de volta** é o prazo de falha do Juiz mais o backoff (ver
  [ressalvas](ressalvas.md), "Composição do RTO"). Um `MaxDelay` menor encurta
  a volta.
- **Eventos de conexão:** `Connected()` diz se há conexão pronta agora;
  `Done()` fecha no encerramento final, e `Err()` dá a causa.

## Erros

| Erro | Quando |
|---|---|
| `ErrHandshake` | `CONNECT` recusado ou sem resposta (inclui o vip sem primário disponível) |
| `ErrDisconnected` | A conexão caiu durante a operação; a biblioteca já está reconectando |
| `ErrTemporaryFailure` | O broker pediu nova tentativa; a leitura fica no buffer |
| `ErrRejected` | `SUBACK`/`UNSUBACK` com status diferente de OK (por exemplo, filtro inválido) |
| `ErrReconnectExhausted` | `MaxAttempts` atingido; o cliente encerrou |
| `ErrClosed` | `Close` foi chamado |
| `ErrInvalidArgument` | Argumento inválido (`client_id` vazio, tópico inválido, idade negativa) |

Todos os métodos podem ser usados por várias goroutines ao mesmo tempo
(RF-55).

## Aplicações de exemplo

`cmd/sensor` (simulador) e `cmd/consumidor` usam só esta API e servem de
referência completa: ver `internal/sensor/simulator.go` e
`internal/consumer/consumer.go`.
