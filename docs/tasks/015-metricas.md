# [MÉTRICAS] 015 — Coleta e exposição de métricas

## Objetivo

Coletar as métricas da seção 9 de `requisitos.md` no broker e nas aplicações e
expô-las em formato textual compatível com Prometheus.

## Por que fazer

As métricas validam os RNFs de latência, vazão, justiça e failover e alimentam
os gráficos da Etapa 3. Também permitem distinguir falha temporária, descarte
sem inscritos e perda por overflow (RNF-13).

## O que esta task entrega

- Pacote `internal/metrics` com contadores, medidores e histogramas com
  p50/p95/p99.
- Endpoint HTTP `/metrics` no broker, em formato textual Prometheus.
- Instrumentação das métricas já possíveis no código atual.
- Testes do pacote e do endpoint.

## Dependências

- Depende de: task 008 (issue #8) — profundidade de filas e descartes por
  retenção.
- Depende de: task 009 (issue #9) — sensores ativos.
- Motivo: parte das métricas só existe depois dessas tasks. O pacote em si pode
  começar antes, instrumentando o que já existe.

Bloqueia: task 016 (aplicações de exemplo) e task 017 (cenários de carga).

## Onde implementar

A pasta `internal/metrics/` já existe (placeholder); criar:

- `internal/metrics/metrics.go` — tipos de métrica e registro.
- `internal/metrics/exposition.go` — formato textual Prometheus.
- `internal/metrics/tests/metrics_test.go` — testes.

Ajustar `internal/broker` (instrumentação) e `cmd/broker/main.go` (flag
`-metrics-addr`).

## Como implementar

- Implementar com a biblioteca padrão (`net/http`, `sync/atomic`), sem
  dependências externas.
- Métricas por rótulo quando a seção 9 pede (por sensor, tópico ou assinante).
- Histogramas para `delivery_latency_seconds` e `ack_latency_seconds`.
- Contadores separados para `NO_SUBSCRIBERS`, overflow do sensor e retenção do
  assinante.
- `failover_duration_seconds` fica para quando as tasks 012 e 013 existirem; o
  pacote já deve suportá-la.

## Critérios de aceitação

- [ ] O broker expõe `/metrics` em formato textual Prometheus.
- [ ] As métricas da seção 9 disponíveis no código atual estão instrumentadas.
- [ ] Descarte sem inscritos, overflow e retenção aparecem em contadores
      distintos.
- [ ] Histogramas reportam p50, p95 e p99.
- [ ] O endereço do endpoint é configurável por flag.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-62.
- RNF-13, RNF-21.
- CT-18; seção 9 de `requisitos.md`.

## Sugestão de implementação

- Evitar rótulos de alta cardinalidade sem limite (por exemplo, por `seq`).
- Teste que sobe o broker, publica e confere os valores no `/metrics`.
