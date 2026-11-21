# [EXEMPLOS] 017 — Gerador de carga e falhas e cenários reprodutíveis

## Objetivo

Criar o gerador de carga e falhas e um conjunto de cenários reprodutíveis que
validem os RNFs de desempenho, justiça e failover e produzam os dados da
Etapa 3.

## Por que fazer

As metas da seção 5.1 de `requisitos.md` são propostas que precisam ser
medidas. Sem cenários reprodutíveis não há como mostrar latência, vazão,
justiça e tempo de failover.

## O que esta task entrega

- Gerador capaz de produzir rajadas, sensores lentos ou agressivos,
  consumidores lentos, queda do broker e queda do Juiz.
- Cenários descritos em arquivos de configuração.
- Execução que coleta as métricas e gera resultados tabulados (por exemplo,
  CSV) para os gráficos.
- Relatório de cada cenário comparando o medido com a meta.

## Dependências

- Depende de: task 013 — failover transparente.
- Depende de: task 015 — métricas.
- Depende de: task 016 — simulador e consumidor.
- Motivo: os cenários combinam as aplicações de exemplo, as métricas e a
  injeção de falhas.

Bloqueia: Etapa 3 (gráficos e validação dos RNFs).

## Onde implementar

A pasta `examples/` já existe (placeholder); criar:

- `examples/cenarios/` — um arquivo de configuração por cenário.
- `examples/runner/main.go` — executa um cenário, injeta falhas e coleta
  métricas.

## Como implementar

Cenários mínimos:

| Cenário | Valida |
|---|---|
| Perfil de referência: 200 sensores, 90% baixa taxa e 10% contínua | RNF-01, RNF-03, CT-17 |
| Rajada de 2.000 mensagens | RF-35, RNF-04, CT-08 |
| Sensor agressivo junto de sensores normais | RF-38, RNF-05, CT-09 |
| Consumidor lento | RF-36, RF-37, CT-10 |
| Queda do broker primário | RNF-06, RNF-07, CT-11 |
| Queda do Juiz | RF-48, CT-15 |
| Sensores offline e sono | RF-25, RF-28, CT-05, CT-06 |

- O runner sobe os processos (brokers, Juiz, endereço virtual, simulador e
  consumidor), injeta as falhas em instantes configurados e coleta o
  `/metrics` ao final.

## Decisões tomadas

1. **Cenários em JSON** (`examples/cenarios/*.json`), lidos com
   `encoding/json`. Campos desconhecidos são erro. A restrição de não usar
   JSON vale só para o transporte.
2. **Processos reais:** o runner (`examples/runner`) compila `cmd/`, sobe cada
   componente como processo, injeta falhas com `kill -9`/SIGINT e lê o
   `/metrics` por HTTP.
3. **Versão reduzida e completa:** `examples/cenarios/ci/` roda no
   `./scripts/dev.sh test` (`TestReducedScenarios`, pulado com `-short`); os
   completos são manuais.
4. **Peças adicionadas para os cenários:**
   - rajada no simulador (`-burst-at`, `-burst-size`);
   - atraso por mensagem no consumidor (`-process-delay`);
   - rótulo `group` na latência e nas recebidas do consumidor;
   - `metrics.ParseText` e `metrics.EstimateQuantile` exportados.

Como rodar e ler os resultados: `examples/README.md`.

## Critérios de aceitação

- [ ] Cada cenário roda com um único comando e é reproduzível pela
      configuração (incluindo semente aleatória).
- [ ] É possível reproduzir rajadas, sensores lentos e queda do broker.
- [ ] Os resultados registram latência, vazão, duplicatas, perdas, backlog e
      tempo de failover.
- [ ] Cada cenário informa se a meta correspondente foi atingida.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-61, RF-62.
- RNF-01 a RNF-06, RNF-22.
- CT-05, CT-06, CT-08 a CT-11, CT-15, CT-17, CT-18.

## Sugestão de implementação

- Versão reduzida de cada cenário para rodar no CI; versão completa apenas
  manual.
- Os cenários longos não devem rodar no `./scripts/dev.sh test` padrão.

## Resultados dos cenários completos

Medidos em 20/11/2026, numa única máquina (WSL2, todos os processos locais),
com os arquivos de `examples/cenarios/`.

| Cenário | Meta | Medido | Atingida |
|---|---|---|---|
| `referencia` | RNF-01 latência p95 ≤ 2 s | 3,96 s | ❌ |
| `referencia` | RNF-01 latência p99 ≤ 10 s | 5,15 s | ✅ |
| `referencia` | RNF-03 vazão ≥ 16,5 leituras/s (80% da oferta) | 20,11 leituras/s | ✅ |
| `referencia` | RNF-07 sem perda | 17.926 confirmadas, 0 pendentes | ✅ |
| `rajada` | Rajada de 2.000 gerada | 2.000 | ✅ |
| `rajada` | RNF-04 nenhuma conexão recusada | 0 | ✅ |
| `rajada` | RNF-04 sem perda | 2.560 confirmadas, 0 pendentes | ✅ |
| `justica` | RNF-05 p95 dos normais ≤ 2× a base | 2,47× (99 ms → 243 ms) | ❌ |
| `justica` | Sensores intensos ativos | 178 leituras | ✅ |
| `justica` | Sem perda | 2.538 confirmadas, 0 pendentes | ✅ |
| `consumidor_lento` | RF-37 descartes por retenção | 785 | ✅ |
| `consumidor_lento` | RF-37 lacuna sinalizada ao consumidor | 171 | ✅ |
| `consumidor_lento` | Nenhuma lacuna sem marca | 0 | ✅ |
| `queda_primario` | RNF-06 RTO ≤ 30 s | 6,5 s | ✅ |
| `queda_primario` | RNF-07 sem perda (RPO = 0) | 1.705 confirmadas, 0 pendentes | ✅ |
| `queda_primario` | Failover concluído pelo Juiz | 1 | ✅ |
| `queda_juiz` | RF-48 entregas continuam sem o Juiz | 1.056 | ✅ |
| `queda_juiz` | Sem perda | 1.755 confirmadas, 0 pendentes | ✅ |
| `offline_sono` | RF-28 sensor marcado offline | 24 | ✅ |
| `offline_sono` | RF-27 sensor marcado dormindo | 41 | ✅ |
| `offline_sono` | RF-25 sem perda com desconexões | 1.893 confirmadas, 0 pendentes | ✅ |

**21 de 23 metas atingidas.** As duas não atingidas têm a mesma causa
provável:

- **RNF-01 (p95 de 3,96 s no perfil de referência):** a vazão medida (~20
  leituras/s) fica praticamente no limite da oferta do perfil (20,6/s).
  - Cada leitura passa por fsync no sensor, no primário e na reserva, mais o
    `DELIVER_ACK` replicado, tudo sob um único lock de gravação (ver
    `docs/ressalvas.md`, "Vazão limitada pela gravação síncrona replicada").
  - Perto da saturação, as filas crescem e a latência sobe. O p99 (5,15 s)
    ainda cumpre a meta de 10 s.
- **RNF-05 (justiça, 2,47×):** os sensores intensos disputam o mesmo lock e os
  mesmos fsyncs. A latência absoluta dos normais (243 ms) segue bem abaixo do
  RNF-01, mas a regra relativa não é cumprida.

As alavancas para os dois são as mesmas: gravar e replicar registros em lote
(um fsync por grupo) e não replicar cada `DELIVER_ACK` de forma síncrona.
Ficam como trabalho futuro; as metas não foram ajustadas.
