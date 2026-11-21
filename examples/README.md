# Cenários de carga e falhas

Gerador de carga e falhas do Maktuf (task 017). O runner sobe o sistema
completo como **processos reais**:
- Juiz;
- brokers `b1` e `b2`, com replicação;
- vip (endereço virtual);
- consumidores;
- um processo de sensores por grupo.

Durante o cenário, ele injeta falhas nos instantes configurados e lê o
`/metrics` de todos os processos ao longo do tempo. No fim, diz se cada meta de
`requisitos.md` foi atingida.

## Rodar um cenário

Na raiz do repositório:

```bash
go run ./examples/runner -cenario examples/cenarios/queda_primario.json
```

O runner compila os executáveis de `cmd/`, sobe tudo em portas livres de
`127.0.0.1` (sem colidir com outros processos) e imprime o resultado de cada
meta. O código de saída é:

| Código | Significado |
|---|---|
| 0 | Todas as metas atingidas |
| 1 | Alguma meta não atingida |
| 2 | Erro de execução |

### Cenários

| Arquivo | O que mostra | Duração |
|---|---|---|
| `referencia.json` | Perfil do RNF-03: 200 sensores (90% a 5 min, 10% a 1 s); latência p95/p99 (RNF-01), vazão e perda | 15 min |
| `rajada.json` | Rajada de 2.000 leituras absorvida pelo buffer, sem recusar conexões nem perder leituras (RNF-04) | ~2 min |
| `justica.json` | Sensores com intervalo de 1 ms entram no meio; o p95 dos normais ≤ 2× a base (RNF-05) | ~2 min |
| `consumidor_lento.json` | Retenção do assinante estoura e a lacuna chega com `GAP_BEFORE` (RF-37) | ~2 min |
| `queda_primario.json` | `kill -9` no primário com prazos padrão: RTO ≤ 30 s e RPO = 0 (RNF-06/07) | ~2 min |
| `queda_juiz.json` | `kill -9` no Juiz: os dados continuam fluindo (RF-48) | ~2 min |
| `offline_sono.json` | Desconexões e sono com atraso: `dormindo`/`offline` publicados e nada se perde | ~2,5 min |

`cenarios/ci/` tem versões reduzidas de cada um, com 10 a 20 s de carga. Elas
rodam no `./scripts/dev.sh test` (teste `TestReducedScenarios`) e são puladas
com `go test -short`.

## Resultados

Cada execução cria `data/cenarios/<cenario>-<data>/`, ou o diretório de
`-out`:

| Arquivo | Conteúdo |
|---|---|
| `relatorio.md` | Falhas aplicadas (instante previsto e real), tabela de metas com ✅/❌ e números gerais: medidas, confirmadas, pendentes, recebidas, repetidas, lacunas, maior fila e failovers |
| `resumo.csv` | Uma linha por meta: `cenario,meta,tipo,medido,alvo,atingida,detalhe` |
| `metricas.csv` | Todas as séries ao longo do tempo: `t_s,processo,metrica,rotulos,valor`. Séries por sensor (`sensor_id`) são somadas por processo. É a fonte dos gráficos da Etapa 3 |
| `*.log` | Log de cada processo |
| `dados/` | Stores dos brokers e buffers dos sensores |

Os instantes (`t_s`, falhas, janelas) contam a partir do início dos sensores.

## Formato do cenário (JSON)

```jsonc
{
  "name": "queda_primario",            // prefixo da pasta de resultados
  "description": "...",
  "seed": 46,                          // semente; cada grupo usa seed + índice
  "duration": "90s",                   // tempo de carga
  "scrape_interval": "250ms",          // período de coleta (precisão do RTO)
  "drain": "30s",                      // espera final para drenar buffers e filas
  "cluster": {
    "replicated": true,                // b1 + b2 + vip; false = só b1, sem vip
    "judge": true,                     // Juiz (exige replicated)
    "failure_timeout": "5s",           // vazio = padrão de cada executável
    "check_interval": "500ms",
    "heartbeat_interval": "1s",
    "fence_timeout": "3s",
    "subscriber_queue": 10000,
    "sensor_queue": 32,
    "status_tolerance": "5s"
  },
  "sensors": [                         // um processo cmd/sensor por grupo
    { "group": "sensor", "count": 20, "continuous_ratio": 1,
      "low_interval": "5m", "continuous_interval": "1s", "stagger": true,
      "buffer_size": 10000, "publish_timeout": "5s",
      "failure_prob": 0, "failure_downtime": "10s",
      "sleep_prob": 0, "sleep_duration": "30s", "late_wake_prob": 0, "late_wake_extra": "30s",
      "burst_at": "20s", "burst_size": 2000,
      "start_at": "0s" }
  ],
  "consumers": [ { "client_id": "consumidor-1", "process_delay": "0s" } ],
  "faults": [                          // action: kill | stop | start
    { "at": "30s", "action": "kill", "target": "primario" }
  ],                                   // target: primario | reserva | b1 | b2 | juiz | vip | consumidor:<id> | sensores:<grupo>
  "goals": [ /* ver abaixo */ ]
}
```

Campos desconhecidos são erro, para pegar erros de digitação. `primario` e
`reserva` são resolvidos na hora da falha, pelo `broker_state` de cada broker.

### Metas

| `type` | Campos | Mede |
|---|---|---|
| `latency` | `group`, `quantile`, `window`, `max` (s) | Percentil de `end_to_end_latency_seconds{group}` só das leituras da janela (diferença dos buckets) |
| `latency_ratio` | `group`, `quantile`, `base`, `test`, `max_ratio` | Percentil da janela `test` dividido pelo da `base` |
| `throughput` | `window`, `min` (leituras/s) | Vazão de `messages_received_total` do consumidor na janela |
| `no_loss` | — | Buffers dos sensores vazios no fim, nenhuma lacuna sem marca e recebidas ≥ confirmadas |
| `rto` | `fault` (índice), `max` (s) | Da falha até o consumidor voltar a receber leituras novas |
| `metric` | `process`, `metric`, `labels`, `window` (opcional), `min`/`max` | Valor final (ou variação na janela) de qualquer série; `process: "brokers"` soma b1 e b2 |

`process` vazio nas metas de latência, vazão e RTO usa o primeiro consumidor.

### Como ler os números

- Os percentis são **estimados pelos buckets** do histograma (de 1 ms a 1 h),
  com a mesma regra do `histogram_quantile` do Prometheus.
- A precisão do RTO é o `scrape_interval`.
- O "resumo final" de cada processo de sensores é lido do log dele, porque os
  sensores já encerraram quando as metas são avaliadas.
