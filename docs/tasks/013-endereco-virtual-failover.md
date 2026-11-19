# [FAILOVER] 013 — Endereço virtual e failover transparente

## Objetivo

Fazer sensores e consumidores usarem um único endereço que sempre leva ao broker
primário, de modo que a troca de broker seja transparente para as aplicações.

## Por que fazer

O diagrama de sequência mostra que "sensores e consumidores só conhecem um
endereço (IP virtual)" e que, no failover, a reserva "assume o IP virtual". A
aplicação do sensor e a do consumidor não devem perceber a troca de broker.

## O que esta task entrega

- Mecanismo de endereço virtual que acompanha o broker primário.
- Atualização do endereço quando o Juiz promove a reserva.
- Teste ponta a ponta de failover com sensor e consumidor ativos.
- Medição do tempo de failover (RTO).

## Dependências

- Depende de: task 010 (issue #10) — reconexão automática e restauração de
  inscrições.
- Depende de: task 011 — replicação primário → reserva.
- Depende de: task 012 — Juiz e promoção.
- Motivo: o failover só é transparente com reconexão no cliente, estado
  replicado e promoção coordenada.

Bloqueia: task 014 (retorno do antigo primário) e task 017 (cenário de queda
do broker).

## Onde implementar

Depende da decisão do ponto 1. Na opção sugerida (proxy):

- `internal/vip/proxy.go` — encaminhamento TCP para o primário atual.
- `internal/vip/tests/proxy_test.go` — testes.
- `cmd/vip/main.go` — executável.
- `test/e2e/failover_test.go` — failover ponta a ponta.

Ajustar `docs/diagramas/` e `docs/protocolo.md` se o mecanismo exigir.

## Como implementar

- Criar o endereço virtual conforme a decisão do ponto 1.
- Quando o Juiz promover a reserva, o endereço passa a levar até ela; as
  conexões antigas são encerradas para forçar a reconexão dos clientes.
- O cliente da task 010 reconecta sozinho no mesmo endereço, refaz o `CONNECT`,
  restaura as inscrições e reenvia o buffer pendente.
- No teste: sensor publicando e consumidor recebendo, derrubar o primário,
  verificar que nenhuma leitura com `PERSISTED` se perde, que a ordem por
  sensor se mantém e medir o tempo até a primeira entrega após a queda.

## Pontos a alinhar antes de implementar

1. **Como simular o IP virtual:**
   - **Proxy TCP** (processo que encaminha para o primário atual, atualizado
     pelo Juiz): fiel ao "cliente só conhece um endereço" e reproduzível sem
     privilégios, mas o proxy vira um ponto único de falha.
   - **IP virtual real** (`ip addr add` no Linux): mais fiel ao diagrama, mas
     exige root e não roda no CI.
   - **Lista de endereços no cliente:** simples, mas contradiz o RF-49 (o
     cliente passaria a conhecer a topologia).
2. **Conexões abertas no failover:** encerrar todas imediatamente ou deixar
   expirar pelo timeout do cliente.

## Critérios de aceitação

- [ ] Clientes usam um único endereço e não conhecem a topologia interna.
- [ ] Ao derrubar o primário, sensores e consumidores reconectam sozinhos e
      continuam operando, sem novo `SUBSCRIBE` explícito.
- [ ] Nenhuma leitura confirmada com `PERSISTED` é perdida (RPO = 0).
- [ ] A ordem por sensor é preservada após o failover.
- [ ] O tempo de failover medido no teste fica dentro de RTO ≤ 30 s.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-43, RF-44, RF-45, RF-49.
- RNF-06, RNF-07, RNF-11.
- CE-07, CE-08, CE-17; CT-11, CT-12.

## Sugestão de implementação

- Prazos curtos de heartbeat e backoff no teste para não atrasar o CI.
- Reaproveitar os helpers de `test/e2e` (`startBroker`, marcador de controle).
- Registrar no log o instante da queda e o da retomada para medir o RTO.
