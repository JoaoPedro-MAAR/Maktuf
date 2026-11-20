# [EXEMPLOS] 016 — Simulador de sensores e consumidor de exemplo

## Objetivo

Criar o simulador de múltiplos sensores (`cmd/sensor`) e o serviço consumidor
de exemplo (`cmd/consumidor`), usando apenas as operações da biblioteca
cliente.

## Por que fazer

São as aplicações que demonstram o middleware funcionando no cenário A e que
geram os dados dos testes de desempenho. Também validam que a biblioteca é
suficiente para as aplicações (RF-51).

## O que esta task entrega

- Simulador com quantidade, taxa, perfis de sensor, falhas, sono e reconexão
  configuráveis.
- Buffer persistente por sensor simulado (task 006).
- Consumidor que se inscreve, recebe, deduplica, detecta lacunas e registra
  métricas.
- Configuração reproduzível por flags ou arquivo.

## Dependências

- Depende de: task 009 (issue #9) — `SLEEP` e status.
- Depende de: task 010 (issue #10) — reconexão, reenvio do buffer e
  deduplicação.
- Depende de: task 015 — métricas.
- Motivo: o simulador usa o ciclo completo do sensor e o consumidor precisa
  deduplicar e medir.

Bloqueia: task 017 (cenários de carga e falhas).

## Onde implementar

As pastas `cmd/sensor/` e `cmd/consumidor/` já existem (placeholders); criar:

- `cmd/sensor/main.go` — simulador.
- `cmd/consumidor/main.go` — consumidor.

Se faltar operação na biblioteca (por exemplo, enviar `SLEEP`), adicioná-la em
`internal/client` com testes.

## Como implementar

- Cada sensor simulado publica em `sensores/{id}/temperatura`, usa o buffer
  persistente e remove a leitura apenas com `PERSISTED` ou `NO_SUBSCRIBERS`.
- Perfis de taxa: baixa (1 leitura a cada 5 min) e contínua (1 leitura/s), em
  proporção configurável (perfil de referência do RNF-03: 90%/10%).
- Falhas: desconexões aleatórias com probabilidade configurável.
- Sono: envia `SLEEP(duração)`, desconecta e volta (dentro ou fora do prazo).
- Consumidor: inscreve-se em `sensores/+/temperatura` e no tópico de status,
  deduplica por `sensor_id + seq`, detecta saltos de `seq` e registra latência
  (`agora - timestamp`).

## Pontos a alinhar antes de implementar

1. **Formato do arquivo de configuração:** flags apenas ou arquivo (JSON ou
   outro). A restrição de "não usar JSON" vale para o transporte, não para a
   configuração.
   **Resolvido:** apenas flags, como no `cmd/broker`.
2. **Política para lacunas (RF-22):** apenas registrar ou também solicitar
   recuperação.
   **Implementado como "apenas registrar"; aguarda confirmação:** o protocolo
   não tem mensagem para pedir recuperação. O consumidor separa lacuna com
   `GAP_BEFORE` (retenção do assinante) de lacuna sem marca (overflow do
   buffer do sensor ou perda).
3. **Tamanho do buffer do sensor** (ponto em aberto 5 de `requisitos.md`):
   flag `-buffer-size` com default provisório de 10000 leituras. O ponto
   continua em aberto.

## Critérios de aceitação

- [ ] O simulador permite configurar quantidade, taxa, falhas, sono e
      reconexão.
- [ ] Leituras sobrevivem a reiniciar o simulador.
- [ ] O consumidor se inscreve, recebe, deduplica e registra métricas.
- [ ] As duas aplicações usam apenas as operações da biblioteca cliente.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-22, RF-51, RF-59, RF-60.
- RNF-15, RNF-22.

## Sugestão de implementação

- Semente aleatória configurável para tornar as falhas reproduzíveis.
- Um diretório de buffer por sensor simulado.
- Teste curto que sobe broker, simulador e consumidor por alguns segundos e
  confere ausência de perdas.
