# [REPLICAÇÃO] 011 — Replicação síncrona primário → reserva

## Objetivo

Replicar no broker reserva as leituras e as inscrições aceitas pelo primário,
enviando `PUBACK(PERSISTED)` e `SUBACK` somente depois do `REPLICA_ACK` da
reserva.

## Por que fazer

Hoje o `PERSISTED` garante apenas a gravação em disco no primário
(`docs/protocolo.md`, seção PUBLISH). Se o primário cair, leituras já
confirmadas ao sensor se perdem. A replicação antes do ACK é o que garante
RPO = 0 e é a base do broker duplicado previsto no Etapa 0 e 1.

## O que esta task entrega

- Conexão de replicação do primário para a reserva.
- `REPLICA` de leituras, inscrições e cancelamentos antes da confirmação ao
  cliente.
- Reserva que aplica as réplicas no próprio store persistente e não atende
  clientes enquanto não for promovida.
- Comportamento definido quando a reserva está indisponível.
- Testes de replicação e de reserva fora do ar.

## Dependências

- Depende de: task 007 (issue #7) — store persistente do broker.
- Motivo: a reserva reaproveita o mesmo store para aplicar as réplicas.

Bloqueia: task 012 (Juiz), task 013 (failover transparente) e task 014
(ressincronização).

## Onde implementar

A pasta `internal/replication/` já existe (placeholder); criar:

- `internal/replication/primary.go` — envio de `REPLICA` e espera do
  `REPLICA_ACK`.
- `internal/replication/replica.go` — recepção e aplicação na reserva.
- `internal/replication/tests/replication_test.go` — testes.

Ajustar:

- `internal/broker/publish.go` e `internal/broker/session.go` — aguardar a
  réplica antes de `PUBACK`/`SUBACK`/`UNSUBACK`.
- `cmd/broker/main.go` — flags de papel (`primario`/`reserva`) e endereço de
  replicação.
- `docs/protocolo.md` — remover a nota "Estado atual" do PUBLISH e descrever o
  fluxo de replicação.

## Como implementar

- O primário mantém uma conexão TCP com a reserva, usando o protocolo próprio
  (`REPLICA`/`REPLICA_ACK`, já definidos em `internal/protocol`).
- Leitura: persistir no primário, enviar `REPLICA(kind=1)`, aguardar
  `REPLICA_ACK` com o mesmo `sensor_id + seq` e só então enfileirar e responder
  `PUBACK(PERSISTED)`.
- Inscrição e cancelamento: `REPLICA(kind=2/3)` antes de `SUBACK`/`UNSUBACK`.
- A reserva grava a réplica no próprio store (com fsync) antes do
  `REPLICA_ACK`.
- A reserva recusa `CONNECT` de clientes enquanto não for promovida.
- Preservar a ordem por sensor: réplicas de um mesmo sensor são enviadas e
  confirmadas em ordem de `seq`.

## Pontos a alinhar antes de implementar

1. **Reserva indisponível:** pelo RF-41, sem confirmação da réplica não há
   `PERSISTED`. O primário deve responder `TEMPORARY_FAILURE` (o sensor mantém a
   leitura no buffer) ou seguir sozinho em modo degradado? A primeira opção é
   fiel ao RF-41/DP-04, mas deixa o sistema sem aceitar leituras enquanto a
   reserva estiver fora.
2. **Cursor de entrega:** o store da task 007 persiste os `DELIVER_ACK`, mas o
   `REPLICA` não tem tipo para eles. Sem replicar o cursor, a reserva reentrega
   leituras já confirmadas pelo consumidor após o failover (duplicatas,
   aceitáveis pelo RNF-10). Replicar o cursor exige um novo `kind` no protocolo.

## Critérios de aceitação

- [ ] `PUBACK(PERSISTED)` só é enviado após o `REPLICA_ACK` da reserva.
- [ ] A reserva possui as leituras e as inscrições confirmadas pelo primário.
- [ ] Inscrições e cancelamentos são replicados antes de `SUBACK`/`UNSUBACK`.
- [ ] Com a reserva fora do ar, nenhuma leitura recebe `PERSISTED` sem réplica
      (ou o comportamento decidido no ponto 1 é aplicado e documentado).
- [ ] A reserva não aceita clientes enquanto não for promovida.
- [ ] `docs/protocolo.md` descreve o fluxo de replicação.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-13, RF-40, RF-41, RF-45.
- RNF-07, RNF-09, RNF-11.
- DP-04, DP-12.

## Sugestão de implementação

- Uma goroutine de escrita e outra de leitura na conexão de replicação, com
  mapa de espera por `sensor_id + seq` (leituras) e `client_id + filtro`
  (inscrições).
- Timeout configurável para o `REPLICA_ACK`.
- Testes com dois `broker.Server` em `127.0.0.1:0`, um em cada papel, usando
  `t.TempDir()` para os stores.
