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
- `flags` é reservado e enviado como `0`, exceto no `DELIVER`, que usa o bit
  `0x0001` (`GAP_BEFORE`). Os demais bits continuam reservados e devem ser
  ignorados na recepção.
- `version` atual: `3`. A versão 2 adicionou o campo `status` ao payload do
  `DELIVER`; a versão 3 trocou o payload de `REPLICA` e `REPLICA_ACK` pelo envio
  do registro do store (seção 9). Mensagens com versão diferente são
  rejeitadas.

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
| `bytes` | `uint32` com o tamanho + bytes opacos (limitado pelo `MaxMessageSize`) |

## 3. Limites e validações

- `MaxMessageSize = 1048576` (1 MiB) para o valor de `length`.
- Um `length` acima do limite é rejeitado antes de alocar memória.
- Mensagem com `length` inconsistente com o tamanho recebido é truncada.
- Payload com bytes sobrando é rejeitado (`ErrTrailingBytes`).
- Versão diferente de `3` é rejeitada (`ErrUnsupportedVersion`).

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
| `0x0C` | `HEARTBEAT` | Broker → Juiz; eco Juiz → Broker |
| `0x0D` | `REPLICA` | Primário → Reserva |
| `0x0E` | `REPLICA_ACK` | Reserva → Primário |
| `0x0F` | `PROMOTE` | Juiz → Broker |

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

Um broker no papel de reserva responde `CONNACK(TEMPORARY_FAILURE)` a qualquer
`CONNECT` e encerra a conexão: ele não atende clientes enquanto não for
promovido (seção 9).

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
- Antes do `PUBACK(PERSISTED, seq)` a leitura é gravada em disco (com fsync)
  no broker, junto com a lista de inscritos para os quais foi enfileirada. Se a
  gravação falhar, o broker encerra a conexão sem responder e o sensor reenvia.
- Com a reserva sincronizada, o `PUBACK(PERSISTED, seq)` só é enviado depois
  do `REPLICA_ACK` da reserva para o registro da leitura (DP-04, seção 9). Com
  a reserva dessincronizada, o primário confirma apenas com o próprio disco.
- O último `seq` de cada sensor também é persistido, então reenvios continuam
  sendo reconhecidos depois de um reinício do broker.
- Cada sessão de sensor tem uma fila limitada de `PUBLISH` aceitos e ainda sem
  `PUBACK`. Quando ela enche, o broker para de ler o socket daquele sensor até
  que o `PUBACK` libere espaço (backpressure). Nenhum `PUBACK` é emitido nesse
  meio-tempo e os outros sensores continuam sendo atendidos (DP-06).

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
- `SUBSCRIBE` e `UNSUBSCRIBE` são gravados em disco e, com a reserva
  sincronizada, replicados antes do `SUBACK`/`UNSUBACK` (seção 9).
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
| `status` | `uint8` (`0` = leitura; `1` online; `2` dormindo; `3` offline) |

Regras do `DELIVER`:

- `timestamp_ms` é calculado pelo broker como `agora - age_ms`, em
  milissegundos Unix UTC.
- Cada inscrito (`client_id`) tem uma fila própria, que existe mesmo com o
  cliente desconectado. A entrega é feita uma leitura por vez: a próxima só é
  enviada depois do `DELIVER_ACK` da atual, o que preserva a ordem de `seq` por
  sensor.
- Uma leitura sem `DELIVER_ACK` é reenviada quando o cliente reconecta
  (pelo menos uma vez); duplicatas são possíveis.
- O broker mantém, por inscrito, um cursor com o último `seq` confirmado por
  `DELIVER_ACK` de cada `sensor_id`. O cursor só avança com o `DELIVER_ACK` da
  leitura pendente e é gravado em disco antes de a próxima leitura ser
  enviada. Uma leitura com `seq` já coberto pelo cursor não é entregue de
  novo.
- Inscrições, filas e cursores sobrevivem a reinício do broker: após o
  reinício, a entrega retoma da primeira leitura não confirmada de cada
  inscrito, sem que ele precise refazer o `SUBSCRIBE`.
- Cada fila de assinante tem retenção limitada. Quando a retenção é excedida, o
  broker descarta a leitura mais antiga **depois da cabeça** (a cabeça pode
  estar em trânsito aguardando `DELIVER_ACK`) e registra o descarte, sem
  bloquear o sensor nem os demais assinantes (RF-36, DP-07).
- A próxima leitura do mesmo sensor entregue àquele assinante leva
  `flags & 0x0001` (`GAP_BEFORE`): antes dela, leituras daquele sensor foram
  descartadas por retenção. A flag não informa quais `seq` foram perdidos nem
  distingue a causa da lacuna (RF-37, CE-11).
- A flag e a marcação de lacuna pendente são persistidas: a lacuna continua
  explícita após reinício do broker e a reconexão do assinante. O contador de
  descartes fica só em memória e é zerado no reinício.
- O descarte por retenção pode remover uma leitura que o sensor já recebeu como
  `PERSISTED`. Essa tensão com a garantia "pelo menos uma vez" está registrada
  em [`ressalvas.md`](ressalvas.md).
- `status != 0` identifica uma transição de estado publicada pelo broker no
  tópico `sensores/{id}/status` (RF-21). Nesse caso `temperature` não tem
  significado e deve ser ignorada. O `seq` do status é gerado pelo broker e
  pertence a um fluxo separado do `seq` das leituras do mesmo sensor; os dois
  não interferem no cursor de entrega.
- As transições de status têm a mesma garantia at-least-once das leituras:
  passam pelo store e pela fila do assinante, sobrevivem a reinício e também
  podem gerar `GAP_BEFORE` se a retenção do assinante for excedida.
- O tópico de status segue as mesmas regras de ciclo de vida dos tópicos de
  leitura: é criado por inscrição, removido quando o último inscrito cancela e
  não faz replay para novos inscritos. Transição sem inscritos é descartada.

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

Regras do `SLEEP`:

- O broker marca o sensor como `dormindo` e publica a transição no tópico
  `sensores/{id}/status`.
- O prazo de retorno é `agora + duration_ms + tolerância`. A tolerância é
  configurável no broker.
- Se o prazo expirar sem novo contato, o monitor publica `offline` (RF-28,
  CE-05). Uma nova conexão válida publica `online` e cancela o prazo pendente
  (RF-29, CE-06).
- O `SLEEP` não tem resposta no protocolo.
- Queda de conexão sem `SLEEP` não marca `offline` nesta versão; a decisão está
  registrada em [`ressalvas.md`](ressalvas.md).

### HEARTBEAT

| Campo | Tipo |
|---|---|
| `broker_id` | `string` |
| `state` | `uint8` (ver tabela abaixo) |
| `last_seq` | `uint64` |

| `state` | Significado |
|---|---|
| `0` | Usado só no eco do Juiz |
| `1` | Primário atendendo clientes |
| `2` | Reserva sincronizada (pode ser promovida) |
| `3` | Reserva sem sincronização válida (não pode ser promovida) |
| `4` | Primário isolado: não atende clientes |

`last_seq` é o **índice de replicação** do broker, não um `seq` de sensor:
no primário, o índice do último registro gravado; na reserva, o do último
registro aplicado. O uso está na seção 10.

### REPLICA

Enviado pelo primário na conexão de replicação. O payload depende de `kind`:

| `kind` | Nome | Conteúdo |
|---|---|---|
| `4` | `RECORD` | `index:uint64`, `record:bytes` |
| `5` | `SYNC_START` | `index:uint64` |
| `6` | `SYNC_END` | `index:uint64` |

- `record` é um registro do store do broker exatamente como o primário o
  gravou em disco (inscrição, cancelamento, leitura com a lista de inscritos e
  o `status`, cursor de `DELIVER_ACK`, descarte por retenção e marcas de
  lacuna). O formato do registro é interno ao broker e não faz parte do
  protocolo de clientes.
- `index` identifica o pedido e é devolvido no `REPLICA_ACK`.
- Os `kind` `1` (leitura), `2` (inscrição) e `3` (cancelamento) da versão 2
  foram removidos e são rejeitados (`ErrUnknownReplicaKind`).

### REPLICA_ACK

| Campo | Tipo |
|---|---|
| `status` | `uint8` (`OK` ou `TEMPORARY_FAILURE`) |
| `kind` | `uint8` (`4` ou `6`) |
| `index` | `uint64` |

A reserva responde a cada `RECORD` fora de sincronização e ao `SYNC_END`,
somente depois de gravar em disco (com fsync). `TEMPORARY_FAILURE` indica que
a reserva não conseguiu gravar; o primário trata como falha da reserva.

### PROMOTE

| Campo | Tipo |
|---|---|
| `broker_id` | `string` |

`broker_id` é o broker que deve ser o primário. Quem recebe o próprio id
assume (reserva) ou volta a atender (primário isolado); quem recebe outro id
se isola. Detalhes na seção 10.

## 6. Status

| Valor | Nome | Uso |
|---|---|---|
| `0` | `OK` | Operação aceita |
| `1` | `PERSISTED` | Leitura persistida (e replicada, com a reserva sincronizada) |
| `2` | `NO_SUBSCRIBERS` | Leitura descartada por não haver inscritos |
| `3` | `TEMPORARY_FAILURE` | Falha temporária; cliente deve manter no buffer |

## 7. Exemplo de quadro

`PUBLISH` do sensor `"a"`, `seq = 1`, `age_ms = 1000`, `temperature = 25.5` no
tópico `"sensores/a"`:

```
00 00 00 23         length = 35
03                  version = 3
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

## 9. Replicação primário → reserva

O par replicado tem um broker primário, que atende clientes, e uma reserva, que
só recebe a replicação (DP-12). A conexão de replicação é TCP, aberta pelo
primário para o endereço de replicação da reserva, e usa este mesmo
enquadramento.

### Sincronização

Ao conectar (na primeira vez e a cada reconexão), o primário envia o estado
completo do seu store:

```
SYNC_START(i) → RECORD(i, r1) → … → RECORD(i, rn) → SYNC_END(i)
                                         ← REPLICA_ACK(SYNC_END, i)
```

- Durante a sincronização a reserva não responde aos `RECORD`; ela acumula os
  registros e, no `SYNC_END`, substitui todo o estado anterior por eles,
  gravando em disco antes do `REPLICA_ACK`.
- Nenhum registro novo é gravado no primário enquanto a sincronização está em
  andamento; por isso o estado enviado é consistente.
- Só depois do `REPLICA_ACK(SYNC_END)` a reserva passa a ser considerada
  **sincronizada**.

### Replicação contínua

Com a reserva sincronizada, cada registro gravado no store do primário é
replicado na mesma ordem, um de cada vez:

1. o primário grava o registro no próprio disco (fsync);
2. envia `RECORD(index, registro)` e espera `REPLICA_ACK(RECORD, index)`;
3. só então conclui a operação e responde ao cliente (`PUBACK(PERSISTED)`,
   `SUBACK` ou `UNSUBACK`) ou enfileira a leitura para os inscritos.

A ordem por sensor (RNF-11) é preservada porque os registros são replicados na
ordem em que foram gravados no primário.

### Reserva indisponível

Se a reserva não responder dentro do timeout configurado, responder
`TEMPORARY_FAILURE` ou a conexão cair, o primário:

- marca a reserva como **dessincronizada** e registra o evento em log;
- conclui a operação em andamento e as seguintes apenas com o próprio disco
  (modo degradado: `PERSISTED` deixa de implicar cópia na reserva);
- tenta reconectar com backoff e, ao conseguir, refaz a sincronização
  completa.

Uma reserva dessincronizada não deve ser promovida: ela não tem as leituras
aceitas enquanto estava fora. As consequências do modo degradado estão em
[`ressalvas.md`](ressalvas.md).

Com o Juiz configurado, o modo degradado depende do aval dele: cada registro
gravado sem reserva só é confirmado ao cliente depois que o Juiz ecoar um
`HEARTBEAT` com `last_seq` maior ou igual ao índice daquele registro. Sem eco
dentro do prazo de isolamento, a operação falha (sem `PUBACK`, o sensor mantém
a leitura no buffer) e o primário se isola (seção 10).

## 10. Juiz

O Juiz coordena o failover do par primário/reserva (DP-12). Ele não recebe
leituras e não está no caminho dos dados (RF-48).

### Conexão e heartbeat

- Cada broker abre uma conexão TCP com o endereço do Juiz e envia
  `HEARTBEAT(broker_id, state, last_seq)` a cada intervalo configurado e
  sempre que o estado muda. Se a conexão cair, o broker só registra o erro e
  reconecta com backoff.
- O Juiz responde cada `HEARTBEAT` com um eco: `HEARTBEAT(broker_id = "juiz",
  state = 0, last_seq = o recebido)`. O eco é o que permite ao broker saber
  que o Juiz está vivo, já que uma escrita TCP para um Juiz inalcançável pode
  parecer bem-sucedida por minutos.
- O Juiz **não** ecoa um primário (`state` 1 ou 4) que não seja o ativo: sem
  eco, esse broker não consegue confirmar escritas sem reserva.
- `broker_id = "juiz"` é reservado.

### Índice de replicação

O índice do primário começa no relógio (nanossegundos Unix) e avança a cada
registro gravado, com ou sem reserva, crescendo também entre reinícios. A
sincronização completa leva o índice atual, e cada `RECORD` leva o seu. Assim,
a reserva sincronizada tem o mesmo índice do primário, e índice da reserva
menor que o do primário indica registros que faltam nela.

### Promoção

O Juiz avalia o estado a cada período configurado e envia `PROMOTE(R)` à
reserva R quando, ao mesmo tempo:

1. já passou o período de aprendizado (um prazo de falha desde que o Juiz
   subiu), para não decidir sem ter ouvido os brokers;
2. o primário ativo está sem `HEARTBEAT` há mais que o prazo de falha;
3. R tem `HEARTBEAT` recente com `state = 2`;
4. o `last_seq` de R é maior ou igual ao último `last_seq` do primário.

A reserva **recusa** o `PROMOTE` enquanto a conexão de replicação com o
primário estiver aberta (o primário está vivo, por exemplo numa partição só
entre Juiz e primário). O Juiz repete o `PROMOTE` a cada período. Ao aceitar,
a reserva fecha o endereço de replicação, monta em memória o estado do store e
passa a aceitar `CONNECT` e a anunciar `state = 1`; nesse momento o Juiz
considera o failover concluído. Se tiver o endereço de replicação do par, a
nova primária passa a tentar replicar para ele, com backoff, até ele voltar
como reserva.

### Início, rebaixamento e retorno do antigo primário

Os dois brokers recebem as mesmas duas flags de replicação, invertidas: o
próprio endereço (`-replication-addr`, usado como reserva) e o do par
(`-replica-addr`, usado como primário). `-role` é só o papel inicial.

- **Início:** com Juiz configurado, um broker que sobe como primário não
  atende clientes; anuncia `state = 4` e espera `PROMOTE` com o próprio id.
  Ele também passa a atender se a reserva sincronizar nele, o que prova que o
  par não é primário.
- **Rebaixamento:** um primário (atendendo, isolado ou aguardando) que recebe
  `PROMOTE` com o id de outro broker vira **reserva**:
  1. fecha as sessões e para de replicar;
  2. descarta o estado em memória;
  3. abre o próprio endereço de replicação e anuncia `state = 3`.
- **Ressincronização:** a primária atual conecta e envia o estado completo
  com o fluxo da seção 9 (`SYNC_START → RECORD… → SYNC_END`). A reserva
  substitui todo o store pelo que recebeu, descartando o estado antigo, e
  passa a anunciar `state = 2`. Daí em diante, recebe as réplicas
  incrementais e pode ser promovida num novo failover.

Assim, depois de um failover, o antigo primário volta como reserva sem
intervenção manual além de reiniciá-lo com o mesmo comando (RF-47).

### Isolamento (anti split-brain)

O primário deixa de atender clientes (fecha as sessões e responde
`CONNACK(TEMPORARY_FAILURE)`) e anuncia `state = 4` quando:

- a reserva está dessincronizada **e** o Juiz não dá o aval de uma escrita
  dentro do prazo de isolamento; ou
- acabou de subir e ainda espera o Juiz.

Ele volta a atender quando recebe `PROMOTE` com o próprio id ou quando a
reserva volta a sincronizar. Um primário que recebe `PROMOTE` com o id de
outro broker não fica isolado: vira reserva (subseção anterior). O Juiz envia `PROMOTE(ativo)`
a todo broker que anuncie `state` 1 ou 4 sem ser o ativo, e `PROMOTE(ativo)`
ao próprio ativo isolado.

O prazo de isolamento dos brokers deve ser menor que o prazo de falha do
Juiz. Os payloads de `HEARTBEAT` e `PROMOTE` não mudaram em relação à versão
3; esta seção só define os valores e os sentidos.

### Endereço virtual

Os clientes conhecem um único endereço, o do vip (`cmd/vip`), e não sabem
quantos brokers existem nem qual é o primário (RF-49). Para cada conexão, o
vip:

1. lê o primeiro quadro do cliente, que precisa ser `CONNECT`;
2. repassa esse `CONNECT` aos brokers configurados, começando pelo último que
   aceitou, até um responder `CONNACK(OK)`. A reserva e o primário isolado
   respondem `TEMPORARY_FAILURE`, então só o primário atual é escolhido;
3. devolve `CONNACK(OK)` ao cliente e, a partir daí, só copia bytes nos dois
   sentidos, sem interpretar o protocolo.

Se nenhum broker aceitar, o vip responde `CONNACK(TEMPORARY_FAILURE)` e o
cliente tenta de novo com backoff. Quando a conexão com o broker cai (primário
morto ou isolado), o vip fecha a do cliente, e a biblioteca reconecta,
restaura as inscrições e reenvia o buffer do sensor (RF-44). O vip não fala
com o Juiz e não exige mudança no protocolo.

