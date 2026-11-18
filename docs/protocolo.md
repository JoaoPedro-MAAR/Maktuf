# Protocolo de aplicação do Maktuf

O protocolo é próprio, binário e inspirado no MQTT, transportado sobre TCP. Ele
é a fonte única de verdade usada pelo broker, pela biblioteca cliente e pela
replicação. A implementação de referência fica em
[`internal/protocol`](../internal/protocol).

## 1. Enquadramento

O TCP entrega um fluxo contínuo de bytes e não preserva as fronteiras entre
mensagens: uma leitura do socket pode trazer meia mensagem, uma mensagem inteira
ou várias mensagens coladas. Por isso toda mensagem começa com o tamanho do seu
corpo.

```
+----------+---------+------+-------+------------------+
| length:4 | version | type | flags |     payload      |
|  uint32  |  uint8  | uint8| uint16|   length-4 bytes |
+----------+---------+------+-------+------------------+
```

- `length` = quantidade de bytes **depois** do campo `length`
  (`version + type + flags + payload`), ou seja, no mínimo 4.
- Todos os inteiros são **big-endian**.
- `flags` é reservado e atualmente enviado como `0`.
- `version` atual: `1`.

O leitor deve usar `bufio.Reader` e `io.ReadFull`: o primeiro guarda bytes
excedentes para a próxima leitura, o segundo espera até completar exatamente N
bytes. Isso resolve, respectivamente, mensagens coladas e leituras parciais.

## 2. Tipos primitivos

| Tipo | Codificação |
|---|---|
| `uint8` | 1 byte |
| `uint16` | 2 bytes big-endian |
| `uint32` | 4 bytes big-endian |
| `uint64` | 8 bytes big-endian |
| `int64` | 8 bytes big-endian (complemento de dois) |
| `float32` | 4 bytes big-endian (IEEE 754) |
| `string` | `uint16` com o tamanho + bytes UTF-8 (máximo 65535 bytes) |

## 3. Limites e validações

- `MaxMessageSize = 1048576` (1 MiB) para o valor de `length`.
- Um `length` acima do limite é rejeitado antes de alocar memória.
- Mensagem com `length` inconsistente com o tamanho recebido é truncada.
- Payload com bytes sobrando é rejeitado (`ErrTrailingBytes`).
- Versão diferente de `1` é rejeitada (`ErrUnsupportedVersion`).

## 4. Tipos de mensagem

| `type` | Nome | Direção |
|---|---|---|
| `0x01` | `CONNECT` | Cliente → Broker |
| `0x02` | `CONNACK` | Broker → Cliente |
| `0x03` | `PUBLISH` | Cliente → Broker |
| `0x04` | `PUBACK` | Broker → Cliente |
| `0x05` | `SUBSCRIBE` | Cliente → Broker |
| `0x06` | `SUBACK` | Broker → Cliente |
| `0x07` | `UNSUBSCRIBE` | Cliente → Broker |
| `0x08` | `UNSUBACK` | Broker → Cliente |
| `0x09` | `DELIVER` | Broker → Cliente |
| `0x0A` | `DELIVER_ACK` | Cliente → Broker |
| `0x0B` | `SLEEP` | Sensor → Broker |
| `0x0C` | `HEARTBEAT` | Broker → Juiz |
| `0x0D` | `REPLICA` | Primário → Reserva |
| `0x0E` | `REPLICA_ACK` | Reserva → Primário |
| `0x0F` | `PROMOTE` | Juiz → Reserva |

## 5. Payloads

### CONNECT

| Campo | Tipo |
|---|---|
| `role` | `uint8` (`1` = sensor, `2` = consumidor) |
| `client_id` | `string` |

### CONNACK

| Campo | Tipo |
|---|---|
| `status` | `uint8` |

### PUBLISH

| Campo | Tipo |
|---|---|
| `topic` | `string` |
| `sensor_id` | `string` |
| `seq` | `uint64` |
| `age_ms` | `uint32` |
| `temperature` | `float32` |

Regras do `PUBLISH`:

- `topic` deve ser um tópico concreto: mesmas regras do `topic_filter`, sem
  curingas. `sensor_id` não pode ser vazio. Publicação inválida faz o broker
  encerrar a conexão, sem responder.
- Sem inscritos cujo filtro case com `topic`, o broker descarta a leitura e
  responde `PUBACK(NO_SUBSCRIBERS, seq)`.
- Com inscritos, a leitura é enfileirada uma vez para cada inscrito (mesmo que
  ele case por mais de um filtro) e o broker responde `PUBACK(PERSISTED, seq)`.
- O broker guarda o último `seq` enfileirado de cada `sensor_id`. Uma leitura
  com `seq` menor ou igual é tratada como reenvio: responde
  `PUBACK(PERSISTED, seq)` sem enfileirar de novo.
- Estado atual: filas apenas em memória; persistência e replicação antes do
  `PERSISTED` (DP-04) ainda não foram implementadas.

### PUBACK

| Campo | Tipo |
|---|---|
| `status` | `uint8` |
| `seq` | `uint64` |

### SUBSCRIBE / UNSUBSCRIBE

| Campo | Tipo |
|---|---|
| `topic_filter` | `string` |

Regras do `topic_filter`:

- Níveis são separados por `/`; nenhum nível pode ser vazio (`a//b`, `/a` e
  `a/` são inválidos).
- `+` casa exatamente um nível e deve ocupar o nível inteiro (`a/+/c` é válido;
  `a+b` é inválido).
- `#` (curinga multinível) não é suportado.
- Filtro inválido faz o broker encerrar a conexão, sem responder.
- `SUBSCRIBE` repetido e `UNSUBSCRIBE` de inscrição inexistente são idempotentes
  e respondem `OK`.
- O tópico (identificado pelo filtro) é criado na primeira inscrição e removido
  quando o último inscrito cancela. Inscrições sobrevivem à desconexão do
  cliente e só são removidas por `UNSUBSCRIBE`.
- `UNSUBSCRIBE` afeta apenas leituras publicadas depois dele. Leituras que já
  estavam na fila do inscrito continuam sendo entregues, pois o broker já as
  confirmou ao sensor com `PERSISTED` (garantia "pelo menos uma vez").

### SUBACK / UNSUBACK

| Campo | Tipo |
|---|---|
| `status` | `uint8` |

### DELIVER

| Campo | Tipo |
|---|---|
| `topic` | `string` |
| `sensor_id` | `string` |
| `seq` | `uint64` |
| `timestamp_ms` | `int64` |
| `temperature` | `float32` |

Regras do `DELIVER`:

- `timestamp_ms` é calculado pelo broker como `agora - age_ms`, em
  milissegundos Unix UTC.
- Cada inscrito (`client_id`) tem uma fila própria, que existe mesmo com o
  cliente desconectado. A entrega é feita uma leitura por vez: a próxima só é
  enviada depois do `DELIVER_ACK` da atual, o que preserva a ordem de `seq` por
  sensor.
- Uma leitura sem `DELIVER_ACK` é reenviada quando o cliente reconecta
  (pelo menos uma vez); duplicatas são possíveis.

### DELIVER_ACK

| Campo | Tipo |
|---|---|
| `topic` | `string` |
| `sensor_id` | `string` |
| `seq` | `uint64` |

Um `DELIVER_ACK` que não corresponde à leitura pendente (duplicado ou
desconhecido) é ignorado e não encerra a sessão.

### SLEEP

| Campo | Tipo |
|---|---|
| `duration_ms` | `uint32` |

### HEARTBEAT

| Campo | Tipo |
|---|---|
| `broker_id` | `string` |
| `state` | `uint8` |
| `last_seq` | `uint64` |

### REPLICA

O payload depende de `kind`:

| `kind` | Conteúdo |
|---|---|
| `1` leitura | `topic:string`, `sensor_id:string`, `seq:uint64`, `timestamp_ms:int64`, `temperature:float32` |
| `2` inscrição | `client_id:string`, `topic_filter:string` |
| `3` cancelamento | `client_id:string`, `topic_filter:string` |

### REPLICA_ACK

| Campo | Tipo |
|---|---|
| `status` | `uint8` |
| `kind` | `uint8` |
| leitura | `sensor_id:string`, `seq:uint64` |
| inscrição/cancelamento | `client_id:string`, `topic_filter:string` |

### PROMOTE

| Campo | Tipo |
|---|---|
| `broker_id` | `string` |

## 6. Status

| Valor | Nome | Uso |
|---|---|---|
| `0` | `OK` | Operação aceita |
| `1` | `PERSISTED` | Leitura persistida e replicada |
| `2` | `NO_SUBSCRIBERS` | Leitura descartada por não haver inscritos |
| `3` | `TEMPORARY_FAILURE` | Falha temporária; cliente deve manter no buffer |

## 7. Exemplo de quadro

`PUBLISH` do sensor `"a"`, `seq = 1`, `age_ms = 1000`, `temperature = 25.5` no
tópico `"sensores/a"`:

```
00 00 00 23         length = 35
01                  version = 1
03                  type = PUBLISH
00 00               flags
0A 73 65 6E 73 6F 72 65 73 2F 61   "sensores/a"
01 61               "a"
00 00 00 00 00 00 00 01            seq = 1
00 00 03 E8                        age_ms = 1000
41 CC 00 00                        temperature = 25.5
```

## 8. Versionamento

Alterações incompatíveis no payload devem incrementar `version`. O receptor
rejeita versões que não conhece, permitindo evoluir o protocolo sem ambiguidade.
