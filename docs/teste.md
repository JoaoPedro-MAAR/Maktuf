# Roteiro de apresentação

Demonstração ao vivo do Maktuf em cerca de 15 minutos. Cada passo diz o que
fazer, o que aparece na tela e o requisito que ele mostra. Os comandos usam as
portas de [execucao.md](execucao.md).

## Preparação (antes de começar)

```bash
go build -o bin/ ./cmd/...
rm -rf data/                       # começa do zero
```

Use `bin/`, e não `go run`: o `kill -9` precisa acertar o processo do broker.

Organize **7 terminais** (o tmux ou as abas do VS Code ajudam):

| Terminal | Processo | Fica visível? |
|---|---|---|
| T1 | Juiz | Pequeno |
| T2 | Broker b2 (reserva) | Pequeno |
| T3 | Broker b1 (primário) | Pequeno |
| T4 | vip | Pequeno |
| T5 | **Painel** (quem é o primário) | **Grande** |
| T6 | **Consumidor** (`-mostrar`) | **Grande** |
| T7 | **Sensor manual** | **Grande** |

Suba T1 a T4 antes de a apresentação começar:

```bash
# T1
bin/juiz -addr 127.0.0.1:7100 -metrics-addr 127.0.0.1:9103
# T2
bin/broker -addr 127.0.0.1:7001 -role reserva -id b2 -data-dir data/b2 \
  -replication-addr 127.0.0.1:7202 -replica-addr 127.0.0.1:7201 -judge-addr 127.0.0.1:7100 \
  -subscriber-queue 20
# T3
bin/broker -addr 127.0.0.1:7000 -id b1 -data-dir data/b1 \
  -replication-addr 127.0.0.1:7201 -replica-addr 127.0.0.1:7202 -judge-addr 127.0.0.1:7100 \
  -subscriber-queue 20
# T4
bin/vip -addr 127.0.0.1:7070 -backends 127.0.0.1:7000,127.0.0.1:7001
# T5: painel (1 = primário, 2 = reserva sincronizada, 0 = fora do ar)
watch -n 1 'curl -s 127.0.0.1:9103/metrics | grep ^judge_broker_state'
```

O painel deve mostrar `b1 1` e `b2 2`. Ainda **não** suba o consumidor.

### Alternativa: painel web

No lugar de T6 e T7, dá para usar o painel no navegador (ver
[execucao.md](execucao.md), seção 4.1):

```bash
bin/painel -sensores sala-01,lab-02    # http://127.0.0.1:8080
```

Os passos funcionam igual, com botões no lugar dos comandos. Os brokers
continuam sendo derrubados no terminal (`kill -9`); a coluna "Brokers"
mostra o primário caindo na hora e a reserva assumindo uns segundos depois.
Para o passo 2 (`NO_SUBSCRIBERS`), cancele antes a inscrição
`sensores/+/temperatura` na coluna do consumidor.

## 1. Arquitetura (1 min)

Mostre o diagrama `docs/diagramas/arquitetura_componentes_v2.png` e o painel:
- dois brokers, primário e reserva com replicação síncrona;
- um Juiz que decide quem é o primário;
- o vip, único endereço que os clientes conhecem.

## 2. Publicar sem ninguém inscrito (RF-07)

```bash
# T7
bin/sensor-manual -addr 127.0.0.1:7070 -id sala-01
```

Digite `20`. Resposta: `NO_SUBSCRIBERS: ninguem inscrito; o broker descartou`.
**Ponto:** o publicador fica sabendo do descarte; a perda nunca é silenciosa.

## 3. Consumidor e publicação (RF-02, RF-03, RF-18, RF-19)

```bash
# T6
bin/consumidor -addr 127.0.0.1:7070 -mostrar
```

Em T7, digite `23.5`. O sensor mostra `PERSISTED` e o consumidor mostra a
linha com seq, temperatura e "medida há X ms". **Ponto:** o sensor envia a
idade da leitura e o broker calcula o instante, sem depender de relógio
sincronizado.

## 4. Rajada (RF-35)

Em T7: `rajada 200 21`. O sensor grava as 200 no buffer e envia em ordem; o
consumidor recebe todas, com seq crescente e sem lacunas. Leva **uns 10 s**
(cada leitura passa por fsync no sensor, no primário e na reserva). Espere
aparecer `rajada enviada` antes do próximo comando.

## 5. Sensor offline, buffer persistente e reenvio em ordem (RF-23 a RF-25)

Em T7:
```
desconectar
22
23
24
pendentes
conectar
```

Enquanto desconectado, as leituras ficam no buffer em disco (`pendentes`
lista os seq). No `conectar`, aparece "reenviando 3 leituras pendentes" e o
consumidor recebe as três, em ordem. Espere elas aparecerem no consumidor
antes do passo seguinte; se não, elas só saem quando o sensor acordar (sem
perda, mas confunde a plateia).

**Extra:** saia do sensor (`sair`) com leituras pendentes e abra de novo com o
mesmo comando. "buffer reaberto" mostra que o buffer sobreviveu ao reinício.

## 6. Sono e estado do sensor (RF-21, RF-27 a RF-29)

Em T7: `dormir 5s`. O consumidor mostra `sala-01 agora esta DORMINDO`.
Espere uns 15 s sem digitar: o prazo (5 s) mais a tolerância (5 s) passa e
aparece `agora esta OFFLINE`. Digite `25`: o sensor acorda e aparece
`agora esta ONLINE`, seguido da leitura.

## 7. Queda do primário e failover transparente (RF-43 a RF-45, RNF-06)

Este é o ponto principal. Em outro terminal:

```bash
kill -9 $(pgrep -f "id b1")
```

- **Painel:** em ~5 s, `b1 0` e `b2 1`. O Juiz promoveu a reserva.
- **T3:** o broker morreu.
- **T7:** digite `26` logo depois do kill. Pode aparecer "sem PUBACK por
  enquanto; fica no buffer". Repita depois de alguns segundos: `PERSISTED`.
- **T6:** o consumidor **nunca foi reiniciado** e continua recebendo, sem
  lacuna. Uma leitura pode aparecer como "repetida (descartada pela
  deduplicação)", o que é o "pelo menos uma vez" funcionando.

**Ponto:** nenhum cliente sabe que o broker mudou. A leitura confirmada antes
da queda está na reserva (RPO = 0).

## 8. O antigo primário volta como reserva (RF-47)

Em T3, suba o b1 de novo **com o mesmo comando**. No log aparecem
`aguardando o juiz` e depois `antigo primario volta como reserva`. O painel
vai para `b1 2` e `b2 1`: o par está pronto para outra falha. Se der tempo,
repita o passo 7 matando o b2 (`pgrep -f "id b2"`).

## 9. Inscrições ao vivo e retenção (RF-03, RF-04, RF-06, RF-09, RF-36, RF-37)

Num terminal extra (T8), suba um consumidor controlado pelo teclado:

```bash
bin/consumidor-manual -id painel
```

| Comando em T8 | Em T7 (sensor), digite | O que mostra |
|---|---|---|
| `inscrever sensores/sala-01/temperatura` | `30` | A leitura aparece também no T8. Só chega o que for publicado **depois** da inscrição (sem replay, RF-06) |
| `inscrever sensores/+/status` | `dormir 3s` | O curinga `+` pega o status de qualquer sensor (RF-09) |
| `pausar` | `rajada 150 31` | O T8 para de consumir; a fila dele cresce no broker |
| `retomar` | — | Chega o aviso `ATENCAO: ... perdida(s) por retencao da fila (GAP_BEFORE)` e depois o resto, em ordem (RF-37). O T6 não perdeu nada: cada consumidor tem a própria fila (RF-36) |
| `cancelar sensores/sala-01/temperatura` | `32` | O T8 para de receber (RF-04); o T6 continua |

O aviso de retenção aparece porque a preparação sobe os brokers com
`-subscriber-queue 20` (fila pequena de propósito). Com o padrão (10.000), a
rajada teria de ser enorme.

As inscrições ficam gravadas no broker pelo `-id`: ao sair e voltar com o
mesmo id, o consumidor continua inscrito. Para começar limpo, use outro `-id`.

## 10. Queda do Juiz (RF-48)

Ctrl+C no T1. Em T7, publique algumas leituras: continuam `PERSISTED` e
chegando no consumidor. **Ponto:** o Juiz não está no caminho dos dados; só o
failover fica suspenso até ele voltar.

## 11. Números medidos (2 min)

Mostre a tabela de resultados de
[tasks/017-gerador-carga-falhas.md](tasks/017-gerador-carga-falhas.md) e, se
quiser, rode um cenário curto ao vivo (~20 s):

```bash
go run ./examples/runner -cenario examples/cenarios/ci/queda_primario.json
```

Seja transparente sobre as duas metas não atingidas (RNF-01 p95 e RNF-05):
o sistema fica perto do limite de vazão, porque cada leitura passa por fsync
no sensor, no primário e na reserva. A causa técnica está em
[ressalvas.md](ressalvas.md), seção "Vazão limitada pela gravação síncrona
replicada"; as melhorias possíveis (gravar e replicar em lote, um fsync por
grupo, `DELIVER_ACK` sem replicação síncrona), no fim dos resultados da
[task 017](tasks/017-gerador-carga-falhas.md).

## Se algo der errado

| Problema | Saída rápida |
|---|---|
| `address already in use` | Sobrou processo de um ensaio: `pkill -f bin/broker; pkill -f bin/juiz; pkill -f bin/vip` |
| O b1 não atende ao subir | Ele espera o Juiz ou a reserva; confira T1 e T2 |
| O failover não acontece | O Juiz só decide 5 s depois de subir; não mate o primário logo no início |
| O consumidor não mostra nada | Ele precisa estar no ar antes da leitura (senão é `NO_SUBSCRIBERS`) |
| Estado estranho de ensaios anteriores | Pare tudo e apague `data/` |

## Carga de fundo (opcional)

Para o consumidor não ficar parado entre os passos, suba sensores automáticos
num terminal extra (aparecem como `sensor-001`...):

```bash
bin/sensor -addr 127.0.0.1:7070 -data-dir data/sensores -count 3 -continuous-ratio 1 -continuous-interval 2s
```
