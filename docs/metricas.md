# Métricas do Maktuf

Implementação da seção 9 de [requisitos.md](requisitos/requisitos.md)
(task 015). Cada processo expõe as próprias métricas em HTTP, no formato
textual do Prometheus, quando recebe a flag `-metrics-addr`:

```bash
go run ./cmd/broker     -metrics-addr 127.0.0.1:9100
go run ./cmd/sensor     -metrics-addr 127.0.0.1:9101 -count 20
go run ./cmd/consumidor -metrics-addr 127.0.0.1:9102

curl -s 127.0.0.1:9100/metrics
```

Sem a flag, nenhuma porta é aberta. O pacote `internal/metrics` usa só a
biblioteca padrão.

## Broker

| Métrica | Tipo | Rótulos | Quando muda |
|---|---|---|---|
| `messages_published_total` | counter | `sensor_id`, `topic` | Leitura nova processada, com PERSISTED ou NO_SUBSCRIBERS |
| `messages_duplicated_total` | counter | `sensor_id` | PUBLISH com seq já aceito (CE-16), confirmado sem reprocessar |
| `messages_dropped_no_subscribers_total` | counter | `sensor_id` | Leitura descartada por falta de inscritos (RF-07) |
| `messages_delivered_total` | counter | — | DELIVER_ACK aceito: entrega confirmada pelo consumidor |
| `ack_latency_seconds` | histogram | — | Do PUBLISH lido até o PUBACK escrito |
| `delivery_latency_seconds` | histogram | — | Da leitura enfileirada para o assinante até o primeiro DELIVER escrito |
| `broker_queue_depth` | gauge | `queue` (`sensor`/`subscriber`), `id` | PUBLISH sem PUBACK por sensor; leituras pendentes por assinante |
| `retention_drops_total` | counter | `client_id` | Leitura descartada por exceder a retenção do assinante (RF-37) |
| `backpressure_waits_total` | counter | — | Sessão de sensor parou de ler o socket por fila cheia |
| `topic_count` | gauge | — | Tópicos ativos |
| `active_subscriptions` | gauge | `topic` | Inscritos por tópico (filtro) |
| `active_sensors` | gauge | `state` (`online`/`dormindo`/`offline`) | Sensores conhecidos em cada estado |
| `replica_synced` | gauge | — | 1 com a reserva sincronizada, 0 caso contrário |
| `replica_desyncs_total` | counter | — | Vezes em que a reserva saiu de sincronia |
| `broker_state` | gauge | — | Estado anunciado ao Juiz: 1 primário, 2 reserva sincronizada, 3 reserva sincronizando, 4 isolado |
| `judge_connected` | gauge | — | 1 quando o Juiz respondeu dentro do prazo de isolamento |

A `ack_latency_seconds` inclui a espera por backpressure, a gravação em disco e
a confirmação da réplica. A `delivery_latency_seconds` não inclui rede nem
consumidor, e não mede reenvios nem leituras restauradas do disco depois de
reiniciar o broker.

Contadores e filas ficam em memória e recomeçam do zero quando o processo
reinicia.

## Simulador de sensores

| Métrica | Tipo | Rótulos | Quando muda |
|---|---|---|---|
| `sensor_readings_total` | counter | — | Leitura gerada |
| `sensor_readings_confirmed_total` | counter | — | Leitura removida do buffer por resposta definitiva, inclusive de execuções anteriores |
| `messages_dropped_no_subscribers_total` | counter | — | Medição respondida com NO_SUBSCRIBERS (RF-07) |
| `sensor_publish_timeouts_total` | counter | — | Medição que ficou no buffer sem PUBACK dentro do prazo |
| `sensor_buffer_overflow_total` | counter | `sensor_id` | Leitura descartada por buffer cheio (RF-26, RF-31) |
| `sensor_buffer_pending` | gauge | `sensor_id` | Leituras aguardando resposta no buffer |
| `sensor_connects_total`, `sensor_connect_failures_total` | counter | — | Conexões com o broker e falhas |
| `sensor_simulated_failures_total`, `sensor_sleeps_total`, `sensor_late_wakes_total` | counter | — | Eventos simulados |
| `sensor_burst_readings_total` | counter | — | Leituras geradas pela rajada (`-burst-at`, `-burst-size`) |

## Consumidor de exemplo

| Métrica | Tipo | Rótulos | Quando muda |
|---|---|---|---|
| `messages_received_total` | counter | `group` | Leitura nova aceita |
| `messages_duplicated_total` | counter | — | Leitura repetida descartada por `sensor_id + seq` (RF-14) |
| `messages_old_total` | counter | — | seq já processado, fora da janela de deduplicação (CE-16) |
| `sequence_gaps_total` | counter | `cause` (`retention`/`unmarked`) | Salto de seq (RF-22) |
| `missing_readings_total` | counter | `cause` | seq faltantes nas lacunas |
| `sensor_status_transitions_total` | counter | `state` | Transição recebida em `sensores/{id}/status` |
| `end_to_end_latency_seconds` | histogram | `group` | Da medição no sensor até o recebimento |

`cause="retention"` é a lacuna sinalizada pelo broker com `GAP_BEFORE`.
`cause="unmarked"` é a lacuna sem marca, vinda de antes do broker,
tipicamente overflow do buffer do sensor. Com isso dá para distinguir perda
por overflow de perda por retenção (RF-31, RNF-13).

`group` é o `sensor_id` sem o último trecho depois de `-` (`sensor-007` vira
`sensor`, `agressivo-001` vira `agressivo`). Com isso, a latência de cada grupo
do simulador pode ser comparada (RNF-05) sem um rótulo por sensor.

A `end_to_end_latency_seconds` inclui o tempo que a leitura ficou no buffer do
sensor. Por isso tem nome diferente da `delivery_latency_seconds` do broker.

## Juiz

`go run ./cmd/juiz -metrics-addr 127.0.0.1:9103`

| Métrica | Tipo | Rótulos | Quando muda |
|---|---|---|---|
| `failover_duration_seconds` | histogram | — | Da detecção do primário sem heartbeat até a reserva promovida anunciar estado primário |
| `judge_promotions_total` | counter | `broker_id` | PROMOTE enviado para promover uma reserva (repetido a cada rodada enquanto ela não assume) |
| `judge_broker_state` | gauge | `broker_id` | Último `state` de cada broker; 0 sem heartbeat dentro do prazo de falha |

A `failover_duration_seconds` mede só a parte coordenada pelo Juiz. O RTO
completo (RNF-06) soma o prazo de falha e a reconexão dos clientes (task 013).

## Endereço virtual (vip)

`go run ./cmd/vip -backends 127.0.0.1:7000,127.0.0.1:7001 -metrics-addr 127.0.0.1:9104`

| Métrica | Tipo | Rótulos | Quando muda |
|---|---|---|---|
| `vip_connections` | gauge | — | Pares cliente↔broker ativos |
| `vip_routed_total` | counter | `backend` | Conexão de cliente entregue a um broker; a troca de `backend` marca o failover visto pelos clientes |
| `vip_rejected_total` | counter | — | `CONNECT` recusado porque nenhum broker aceitou: a janela de indisponibilidade |

## Histogramas e percentis

Cada histograma é exposto no formato padrão (`_bucket`, `_sum`, `_count`),
que o Prometheus agrega com `histogram_quantile`. Ele também vem com a família
`<nome>_quantile{quantile="0.5|0.95|0.99"}`, para ler p50, p95 e p99 direto
com `curl`.

Esses percentis são **estimados pelos buckets**, por interpolação linear dentro
do bucket, a mesma regra do `histogram_quantile`. A precisão é limitada pela
largura do bucket. Os limites padrão são 1 ms, 2,5 ms, 5 ms, 10 ms, 25 ms,
50 ms, 100 ms, 250 ms, 500 ms, 1 s, 2,5 s, 5 s, 10 s, 30 s, 60 s, 5 min, 15 min
e 1 h. Um percentil acima do maior limite aparece como 3600.
