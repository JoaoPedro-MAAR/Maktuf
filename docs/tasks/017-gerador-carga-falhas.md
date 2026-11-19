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
