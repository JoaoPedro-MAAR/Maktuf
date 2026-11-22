# Execução do Maktuf

Passo a passo para compilar, subir o ambiente completo, provocar falhas,
rodar os cenários de medição e ler as métricas.

## 1. Pré-requisitos

- **Go 1.22.2 ou superior** (`go version`).
- **Linux, macOS ou WSL.** Os cenários usam sinais POSIX (`kill -9`).
- **`curl`**, para ler as métricas.
- **Opcional:** `golangci-lint` na versão do projeto, instalado em `bin/` por
  `./scripts/dev.sh tools`.

Não há dependências externas além da biblioteca padrão do Go.

## 2. Compilar e testar

```bash
git clone <url-do-repositorio> Maktuf && cd Maktuf
./scripts/dev.sh setup                     # ativa o hook de pre-commit
go build -o bin/ ./cmd/...                 # bin/broker, juiz, vip, sensor, consumidor
./scripts/dev.sh check                     # gofmt, go vet e golangci-lint
./scripts/dev.sh test                      # todos os testes (~3 min, inclui os cenários reduzidos)
go test -short ./...                       # sem os cenários reduzidos (~1 min)
```

| Executável | Papel |
|---|---|
| `bin/broker` | Broker: primário ou reserva, com replicação e link com o Juiz |
| `bin/juiz` | Coordena o failover: heartbeat, detecção e promoção |
| `bin/vip` | Endereço virtual: o único endereço que os clientes conhecem |
| `bin/sensor` | Simulador de N sensores (perfis, falhas, sono, rajada) |
| `bin/consumidor` | Consumidor de exemplo (deduplicação, lacunas, latência); `-mostrar` imprime uma linha legível por leitura |
| `bin/sensor-manual` | Um sensor controlado pelo teclado (publicar, rajada, dormir, cair, reconectar), para demonstrações |
| `bin/consumidor-manual` | Um consumidor controlado pelo teclado (inscrever, cancelar, pausar, retomar), para demonstrações |

Todos aceitam `-h`. Com `-metrics-addr`, cada um expõe `/metrics`.

## 3. Ambiente completo

Abra um terminal por processo, na raiz do repositório. Portas usadas:

| Processo | Clientes | Replicação | Métricas |
|---|---|---|---|
| Juiz | `:7100` (brokers) | — | `:9103` |
| Broker b1 | `:7000` | `:7201` | `:9100` |
| Broker b2 | `:7001` | `:7202` | `:9101` |
| vip | `:7070` | — | `:9104` |
| Consumidor | — | — | `:9102` |
| Sensores | — | — | `:9105` |

```bash
# T1: Juiz
bin/juiz -addr 127.0.0.1:7100 -metrics-addr 127.0.0.1:9103

# T2: broker b2, começa como reserva (suba antes do b1)
bin/broker -addr 127.0.0.1:7001 -role reserva -id b2 -data-dir data/b2 \
  -replication-addr 127.0.0.1:7202 -replica-addr 127.0.0.1:7201 \
  -judge-addr 127.0.0.1:7100 -metrics-addr 127.0.0.1:9101

# T3: broker b1, começa como primário
bin/broker -addr 127.0.0.1:7000 -id b1 -data-dir data/b1 \
  -replication-addr 127.0.0.1:7201 -replica-addr 127.0.0.1:7202 \
  -judge-addr 127.0.0.1:7100 -metrics-addr 127.0.0.1:9100

# T4: endereço virtual
bin/vip -addr 127.0.0.1:7070 -backends 127.0.0.1:7000,127.0.0.1:7001 -metrics-addr 127.0.0.1:9104

# T5: consumidor (suba antes dos sensores; sem inscritos, a leitura é descartada)
bin/consumidor -addr 127.0.0.1:7070 -metrics-addr 127.0.0.1:9102 -report 5s

# T6: sensores (prazos curtos para ver o fluxo; sem flags, vale o perfil do RNF-03)
bin/sensor -addr 127.0.0.1:7070 -data-dir data/sensores -count 10 \
  -continuous-ratio 0.3 -continuous-interval 500ms -low-interval 5s -metrics-addr 127.0.0.1:9105 -report 5s
```

Como funciona:
- **Flags de replicação simétricas:** cada broker recebe o **próprio**
  endereço de replicação (`-replication-addr`, usado como reserva) e o **do
  par** (`-replica-addr`, usado como primário). `-role` é só o papel inicial;
  depois de um failover, os papéis se invertem sozinhos.
- **O b1 não atende antes de a reserva sincronizar nele ou de o Juiz o
  promover:** um broker nunca vira primário sozinho.
- **Os clientes só conhecem o vip** (`:7070`), que é o `-addr` padrão do
  `sensor`, do `consumidor` e do `sensor-manual`. Com um broker sozinho (sem
  vip), passe o endereço dele: `-addr 127.0.0.1:7000`.

Para acompanhar quem é o primário (`1` = primário, `2` = reserva
sincronizada, `3` = reserva sincronizando, `4` = isolado, `0` = fora do ar):

```bash
watch -n 1 'curl -s 127.0.0.1:9103/metrics | grep ^judge_broker_state'
```

Para recomeçar do zero, pare tudo e apague `data/`.

## 4. Provocando falhas na mão

| Experimento | Como | O que observar |
|---|---|---|
| **Queda do primário** (CT-11) | `kill -9` no broker com estado `1` (`pgrep -f "id b1"`) | Em ~5 s o Juiz loga `failover concluido` e o outro broker vira `1`. Consumidor e sensores **não são reiniciados**: seguem pelo vip, com `lacunas_sem_marca=0` |
| **Volta do antigo primário** (RF-47) | Suba de novo o broker morto, **com o mesmo comando** | Ele aguarda o Juiz, vira reserva (`antigo primario volta como reserva`) e chega a `2` depois da ressincronização |
| **Segundo failover** | Com o antigo primário já em `2`, `kill -9` no primário atual | O par volta a trocar, sem perda |
| **Queda do Juiz** (CT-15) | Com a reserva em `2` (sincronizada), Ctrl+C no T1 | Os dados continuam fluindo; só o failover fica suspenso. Sem reserva sincronizada é a linha de baixo |
| **Juiz e reserva fora** | Ctrl+C no Juiz e na reserva | O primário se isola (`state 4`) na próxima escrita e o `pendentes` dos sensores sobe; o Juiz de volta libera tudo |

Cuidado ao usar `pkill -f "id b1"` dentro de um script: o padrão pode casar
com a linha de comando do próprio script.

## 5. Cenários de medição (task 017)

```bash
go run ./examples/runner -cenario examples/cenarios/queda_primario.json
```

O runner sobe tudo como processos em portas livres, injeta as falhas do
cenário, coleta as métricas e escreve `relatorio.md`, `resumo.csv` e
`metricas.csv` em `data/cenarios/<cenario>-<data>/`. Os cenários, o formato
JSON e a leitura dos resultados estão em
[`examples/README.md`](../examples/README.md); os resultados medidos, em
[`tasks/017-gerador-carga-falhas.md`](tasks/017-gerador-carga-falhas.md).

## 6. Métricas

Cada processo com `-metrics-addr` expõe `/metrics` no formato textual do
Prometheus:

```bash
curl -s 127.0.0.1:9100/metrics   # broker
curl -s 127.0.0.1:9102/metrics | grep -E '^(messages_|sequence_gaps|end_to_end.*quantile)'
```

O significado de cada métrica está em [`metricas.md`](metricas.md).
`<nome>_quantile{quantile="0.95"}` traz o percentil estimado de cada
histograma.

## 7. Problemas comuns

| Sintoma | Causa provável |
|---|---|
| `address already in use` | Outra instância rodando; mude as portas ou pare a anterior |
| Cliente repete `status TEMPORARY_FAILURE` | Ele está apontando direto para a reserva (ou para um primário isolado). Use o vip (`-addr 127.0.0.1:7070`), que leva sempre ao primário atual |
| Broker primário não aceita clientes ao subir | Ele espera o Juiz ou a reserva sincronizar nele; confira se o Juiz e a reserva estão no ar |
| Sensores com `sem_inscritos` > 0 | O consumidor subiu depois dos sensores ou sem inscrição; sem inscritos, a leitura é descartada (RF-07) |
| Failover não acontece | O Juiz só decide depois de ouvir os brokers por um prazo de falha completo; logo depois de subir, aguarde |
| Antigo primário não volta como reserva | Falta `-replication-addr` ou `-data-dir` nele (o log diz qual) |
