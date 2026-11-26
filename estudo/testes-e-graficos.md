# Estudo — aplicações de teste, cenários e gráficos

## 1. As três camadas de teste

| Camada | O que é | Como roda | Para quê |
|---|---|---|---|
| **Testes Go** | `go test` em cada pacote (`internal/*/tests/`) | `./scripts/dev.sh test` | Regras isoladas: codec, fila justa, retenção, promoção do Juiz, buffer |
| **Aplicações de exemplo** | `sensor`, `consumidor`, `sensor-manual`, `consumidor-manual`, `painel` | na mão, um terminal por processo | Demonstrar e explorar o sistema |
| **Runner (gerador de carga e falhas)** | `examples/runner` + cenários JSON | `go run ./examples/runner -cenario ...` | Medir o sistema inteiro contra as metas, com falhas reais |

Os gráficos do relatório vêm **só do runner**.

---

## 2. As aplicações de teste

Todas usam **só a biblioteca cliente** (`internal/client`), como uma aplicação
real faria.

| Executável | O que faz |
|---|---|
| `sensor` | Simula N sensores ao mesmo tempo. Perfis: contínuo (1 leitura/s) e baixo (1 a cada 5 min). Sorteia falhas de conexão e sono. Cada sensor tem buffer em disco próprio. `-seed` torna o cenário reproduzível |
| `consumidor` | Se inscreve, recebe, deduplica e mede tudo com o `Tracker`: latência ponta a ponta, duplicatas, fora de ordem, lacunas com e sem aviso. Expõe `/metrics` |
| `sensor-manual` | Um sensor comandado pelo terminal: temperatura, `rajada`, `dormir`, `desconectar`, `conectar` |
| `consumidor-manual` | Um consumidor comandado pelo terminal: `inscrever`, `cancelar`, `pausar`, `retomar` |
| `painel` | Página web (`:8080`) com sensores e consumidor comandados pelo navegador e o estado dos brokers ao vivo |

---

## 3. O runner: como um cenário acontece

```
go run ./examples/runner -cenario examples/cenarios/queda_primario.json
```

1. **Compila** os executáveis de `cmd/` (por isso precisa do Go).
2. **Sobe tudo como processos reais**, em portas livres de `127.0.0.1`:
   Juiz, broker `b1` e `b2` (replicados), vip, consumidores e um processo
   `sensor` por grupo.
3. **Gera carga** pelo tempo do cenário (`duration`).
4. **Injeta as falhas** nos instantes do JSON (por exemplo, `kill -9` no
   primário aos 30 s) e anota o instante previsto e o real.
5. **Coleta o `/metrics` de todos os processos** a cada 250 ms.
6. **Drena** no fim: espera os buffers dos sensores e as filas esvaziarem.
7. **Avalia as metas** e escreve o relatório.

Código de saída: `0` todas as metas atingidas, `1` alguma falhou, `2` erro.

### Saída: `data/cenarios/<cenario>-<data>/`

| Arquivo | Conteúdo |
|---|---|
| `relatorio.md` | Falhas aplicadas, tabela de metas ✅/❌ e números gerais |
| `resumo.csv` | Uma linha por meta |
| `metricas.csv` | Todas as séries no tempo: `t_s,processo,metrica,rotulos,valor` — **fonte dos gráficos** |
| `*.log` | Log de cada processo |

### Tipos de meta que o runner avalia (`examples/runner/goals.go`)

| Tipo | Exemplo |
|---|---|
| `latency` | p95 ≤ 5 s, a partir do histograma `end_to_end_latency_seconds` do consumidor |
| `latency_ratio` | p95 dos normais ≤ 2× a base + 0,5 s (justiça) |
| `throughput` | vazão ≥ 80% da carga oferecida |
| `no_loss` | confirmadas pelos sensores = recebidas pelos consumidores, 0 pendentes |
| `rto` | tempo da falha até o consumidor voltar a receber leituras novas |
| `metric` | uma métrica qualquer contra um limite (ex.: descartes por retenção > 0) |

**Como se prova "sem perda" (RPO = 0):** comparando as duas pontas, que não
caíram. O que os **sensores** tiveram confirmado com PUBACK tem de ter chegado
aos **consumidores**. As métricas do broker não servem: o broker que morreu
leva as dele junto.

**Como se mede o RTO:** do `kill -9` até o primeiro aumento de leituras
recebidas no consumidor. A precisão é o período de coleta, 0,25 s.

---

## 4. Os cenários e o que cada um prova

| Cenário | Carga | Falha | Prova | Resultado |
|---|---|---|---|---|
| `referencia` | 200 sensores: 180 a 1/5 min e 20 a 1/s (20,6 leituras/s), 15 min | — | Latência, vazão e perda no perfil do cenário A | p95 **0,082 s** (meta 5 s); 18.456 confirmadas = recebidas |
| `escala_*` | Mesmo mix com 50 a 600 sensores, 3 min cada | — | Onde o sistema satura | Teto de ~**43 leituras/s** |
| `escala_sem_replica_*` | 200 e 400 sensores, só o `b1` | — | Quanto custa a replicação | Comparação com/sem reserva |
| `rajada` | 20 a 1/s + rajada de 2.000 aos 20 s | — | Buffer absorve a rajada, sem recusar conexão | 0 recusas, 0 perdas |
| `justica` | 20 normais; aos 60 s entram 3 com intervalo de 1 ms | — | Barulhentos não atrasam os normais | p95 dos normais **0,203 s** (limite 0,638 s) |
| `consumidor_lento` | 20 a 1/s, consumidor leva 200 ms/msg, retenção 50 | — | Retenção descarta e avisa | 799 descartes, 86 lacunas avisadas, **0 sem aviso** |
| `queda_primario` | 20 a 1/s | `kill -9` no primário aos 30 s | Failover sem perda | RTO **6,28 s**; 1.706 = 1.706, 0 repetidas |
| `queda_juiz` | 20 a 1/s | `kill -9` no Juiz aos 30 s | Dados seguem sem o Juiz | 1.100 entregas depois da queda |
| `offline_sono` | 20 a 1/s com quedas e sono sorteados | Desconexões | Status dormindo/offline e nada se perde | 44 dormindo, 25 offline, 0 pendentes |

**Total: 21 de 21 metas.**

`examples/cenarios/ci/` tem versões de 10 a 20 s de cada cenário. Elas rodam
dentro do `go test` (`TestReducedScenarios`), então o CI também testa o
sistema inteiro com falhas.

---

## 5. Como os gráficos foram gerados

```
./scripts/dev.sh graficos-setup     # uma vez: cria .venv e instala matplotlib
./scripts/dev.sh graficos           # gera os PNGs
```

1. A bateria final rodou todos os cenários com `-out data/etapa3`.
2. `scripts/graficos.py` (Python + matplotlib) lê o `metricas.csv` da
   execução **mais recente** de cada cenário em `data/etapa3/`.
3. Calcula as séries:
   - **vazão:** diferença do contador de recebidas entre coletas ÷ intervalo;
   - **p95/p99 ao longo do tempo:** estimados pelos buckets do histograma em
     janelas, como o Prometheus faz. Por isso as curvas oscilam um pouco: o
     valor "pula" entre as bordas dos buckets;
   - **estado dos brokers:** a métrica do Juiz (1 primário, 2 reserva
     sincronizada, 3 sincronizando, 0 fora).
4. Desenha com a **linha da meta** em cada gráfico, para comparar com a
   restrição do cenário.
5. Para o antes/depois do ajuste, usa `-antes data/etapa3-antes` (a bateria
   rodada antes do cursor sem fsync).
6. Salva os PNGs em `docs/relatorio/graficos/` e escreve `metas.csv` e
   `escala.csv`.

### O que cada gráfico mostra

| Gráfico | Mostra |
|---|---|
| `referencia_latencia` / `_vazao` / `_fila` | p95/p99 no tempo contra a meta; vazão recebida contra a oferecida; fila do assinante |
| `escala_vazao` / `escala_latencia` | Vazão e p95 por número de sensores: onde satura, com e sem réplica |
| `rajada` | A rajada entrando e o buffer esvaziando sem perda |
| `justica_latencia` / `_vazao` | p95 dos normais antes e durante os barulhentos, com a linha do limite |
| `consumidor_lento` | Descartes por retenção e lacunas avisadas |
| `queda_primario` + `_estados` | Vazão caindo e voltando (RTO) e a troca de papéis dos brokers |
| `queda_juiz` | Entregas seguindo depois da queda do Juiz |
| `offline_sono` | Transições dormindo/offline/online |
| `ajuste_*` | Antes e depois do cursor sem fsync: latência, vazão e referência |

---

## 6. Perguntas prováveis

1. **"Como vocês sabem que nada se perdeu no failover?"** — Comparando as
   pontas: 1.706 leituras confirmadas pelos sensores, 1.706 recebidas pelos
   consumidores, 0 repetidas.
2. **"Por que o teste é reproduzível?"** — Cada cenário tem `seed`; os sorteios
   de falha e sono dependem só dela. As portas são livres, sem colisão.
3. **"O que é a carga do cenário A?"** — 90% dos sensores a 1 leitura/5 min e
   10% contínuos a 1 leitura/s.
4. **"Por que o p95 oscila no gráfico?"** — É estimado pelos buckets do
   histograma, não por amostras exatas.
5. **"Onde o sistema satura e por quê?"** — ~43 leituras/s com réplica; o
   limite é o fsync (~10 ms) no primário e na reserva em cada leitura.
6. **"O que mudou depois da primeira bateria?"** — A latência estava acima da
   meta (p95 9,66 s). O diagnóstico apontou 4 fsyncs em série; tiramos o fsync
   do cursor e o p95 caiu para 0,082 s.
