# [JUIZ] 012 — Juiz: heartbeat, detecção de falha e promoção

## Objetivo

Implementar o Juiz: receber `HEARTBEAT` dos dois brokers, detectar a falha do
primário e enviar `PROMOTE` à reserva quando ela estiver sincronizada.

## Por que fazer

O Etapa 0 e 1 define 2 brokers e 1 Juiz que "checaria se os brokers estão
funcionando normalmente" e informaria "quando o reserva pode assumir". Sem o
Juiz não há failover automático.

## O que esta task entrega

- Executável `cmd/juiz` e pacote `internal/juiz`.
- Envio periódico de `HEARTBEAT` pelos brokers.
- Detecção de primário ausente após prazo configurável.
- Promoção da reserva sincronizada via `PROMOTE`.
- Fluxo de dados independente do Juiz.
- Testes de detecção, promoção e queda do Juiz.

## Dependências

- Depende de: task 011 — replicação primário → reserva.
- Motivo: a promoção só é segura se a reserva tiver as leituras confirmadas.

Bloqueia: task 013 (failover transparente) e task 014 (retorno do antigo
primário).

## Onde implementar

A pasta `internal/juiz/` e `cmd/juiz/` já existem (placeholders); criar:

- `internal/juiz/judge.go` — estado dos brokers e decisão de promoção.
- `internal/juiz/tests/judge_test.go` — testes.
- `cmd/juiz/main.go` — executável com flags.

Ajustar:

- `internal/broker` — cliente de heartbeat e tratamento de `PROMOTE`.
- `docs/protocolo.md` — valores do campo `state` do `HEARTBEAT` e quem conecta
  em quem.

## Como implementar

- Os brokers abrem conexão TCP com o Juiz e enviam
  `HEARTBEAT(broker_id, state, last_seq)` a cada intervalo T.
- O Juiz marca o primário como falho se não receber heartbeat dentro do prazo.
- Antes de promover, o Juiz confere se a reserva está sincronizada (por
  exemplo, `last_seq` da reserva igual ao último informado pelo primário).
- O Juiz envia `PROMOTE(broker_id)` na conexão da reserva; ao recebê-lo, a
  reserva passa a aceitar clientes como primária.
- Falha do Juiz não afeta a publicação nem a entrega: os brokers só registram o
  erro e tentam reconectar ao Juiz.

## Pontos a alinhar antes de implementar

1. **Juiz replicado:** ponto em aberto 11.1 nº 6. A DP-12 fixa 1 Juiz; a
   sugestão é manter 1 Juiz nesta task e registrar o ponto único de falha da
   coordenação.
2. **Valores de `state`:** o protocolo tem o campo `state:uint8` sem valores
   definidos. Definir (por exemplo, `1 = primário`, `2 = reserva
   sincronizada`, `3 = reserva sincronizando`) em `docs/protocolo.md`.
3. **Critério de sincronizado:** comparar `last_seq` global, por sensor ou por
   outro marcador do store.

### Decisões tomadas

1. **Resolvido:** 1 Juiz, com o ponto único de falha da coordenação registrado
   em `docs/ressalvas.md`.
2. **Resolvido:** `state` = `1` primário, `2` reserva sincronizada, `3` reserva
   sem sincronização válida e `4` primário isolado. `0` só no eco do Juiz.
3. **Resolvido:** critério pelo índice de replicação (`last_seq`), que começa
   no relógio do primário e avança a cada registro gravado. A reserva é
   promovida só com `state = 2` e índice maior ou igual ao último do primário.
4. **Anti split-brain:**
   - o Juiz ecoa cada `HEARTBEAT`;
   - o primário só confirma escritas sem reserva com o eco do Juiz e se isola
     sem ele;
   - a reserva recusa `PROMOTE` enquanto o primário replica nela;
   - `PROMOTE(outro id)` isola um primário deposto.

   Detalhes em `docs/protocolo.md`, seção 10.

## Critérios de aceitação

- [ ] Ausência de heartbeat do primário dentro do prazo dispara a avaliação de
      failover.
- [ ] O Juiz promove a reserva somente se ela estiver sincronizada.
- [ ] Após `PROMOTE`, a reserva aceita conexões de clientes.
- [ ] Com o Juiz fora do ar, sensores continuam publicando e consumidores
      continuam recebendo; o failover fica suspenso até o Juiz voltar.
- [ ] Intervalo de heartbeat e prazo de falha são configuráveis por flag.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-42, RF-43, RF-48.
- RNF-06; CE-09; CT-15.
- DP-08, DP-12.

## Sugestão de implementação

- Relógio injetável no Juiz para testar prazos sem `sleep` longo.
- Um único ticker para avaliar prazos.
- Log estruturado de cada transição (broker, estado, `last_seq`).
