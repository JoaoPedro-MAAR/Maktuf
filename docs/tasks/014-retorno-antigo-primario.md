# [REPLICAÇÃO] 014 — Retorno do antigo primário como reserva

## Objetivo

Quando o antigo primário voltar após uma falha, fazê-lo entrar como reserva,
ressincronizar com o primário atual e só então ficar apto a assumir num novo
failover.

## Por que fazer

O Etapa 0 e 1 e o diagrama de sequência dizem que, quando o antigo primário
voltar, "ele entra como reserva e o Juiz coordena a sincronização". Sem isso, o
sistema só aguenta uma falha, e o broker que volta pode tentar atender como
primário ao mesmo tempo que o atual (dois primários).

## O que esta task entrega

- Inicialização do broker que consulta o Juiz antes de atender clientes.
- Ressincronização completa do antigo primário a partir do primário atual.
- Transição de "sincronizando" para "reserva sincronizada" informada ao Juiz.
- Teste de dois failovers seguidos.

## Dependências

- Depende de: task 011 — replicação.
- Depende de: task 012 — Juiz.
- Depende de: task 013 — failover transparente.
- Motivo: a volta do broker reutiliza a replicação e é coordenada pelo Juiz.

Bloqueia: validação completa de alta disponibilidade (RF-40 a RF-50).

## Onde implementar

- `internal/replication/sync.go` — transferência do estado completo.
- `internal/replication/tests/sync_test.go` — testes.
- Ajustes em `internal/juiz` (papel atribuído ao broker que volta) e em
  `internal/broker` (não atender clientes até ser primário).
- `test/e2e/failover_test.go` — cenário de dois failovers.

## Como implementar

- Ao iniciar, o broker se registra no Juiz e recebe seu papel; um broker que
  volta nunca assume como primário sozinho.
- O antigo primário descarta ou reconcilia seu estado local e recebe do
  primário atual o estado completo (leituras pendentes, inscrições, último
  `seq` por sensor e, se decidido na task 011, cursores).
- Após a carga completa, passa a receber as réplicas incrementais e informa ao
  Juiz que está sincronizado.
- O primário atual continua atendendo durante toda a sincronização.

## Pontos a alinhar antes de implementar

1. **Mensagens de sincronização:** reaproveitar o fluxo de `REPLICA` para a
   carga completa ou criar mensagens novas (por exemplo, `SYNC_REQUEST` e
   `SYNC_DONE`). Mensagens novas exigem atualizar `docs/protocolo.md`.
2. **Estado local do antigo primário:** descartar e copiar tudo do atual
   (mais simples e seguro) ou reconciliar.

## Critérios de aceitação

- [ ] O antigo primário volta como reserva e não aceita clientes.
- [ ] Nunca há dois brokers aceitando clientes ao mesmo tempo.
- [ ] A sincronização não interrompe o primário atual.
- [ ] Após sincronizar, o broker fica apto e um segundo failover funciona sem
      perda de leituras confirmadas.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-40, RF-47.
- RNF-07, RNF-09.
- CE-18.

## Sugestão de implementação

- Reaproveitar o `snapshot()` do store da task 007 como formato da carga
  completa.
- Testar com três etapas: falha do primário, volta como reserva e falha do novo
  primário.
