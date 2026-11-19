# [DOCS] 018 — Documentação de execução e da biblioteca

## Objetivo

Documentar como compilar, executar e testar o Maktuf, como reproduzir os
cenários e como usar a biblioteca cliente, de forma que outra pessoa consiga
repetir tudo sem ajuda.

## Por que fazer

O README atual só aponta para os documentos de requisitos. O RF-63 exige que
outra pessoa consiga reproduzir os testes, e o RNF-19 exige documentação de API
da biblioteca com exemplos.

## O que esta task entrega

- README com pré-requisitos, compilação e execução de cada componente.
- Passo a passo para subir o ambiente completo (brokers, Juiz, endereço
  virtual, sensores e consumidor).
- Instruções para rodar cada cenário da task 017 e ler as métricas.
- Documentação da API da biblioteca com exemplos.
- Diagramas e documentos atualizados com o que mudou na implementação.

## Dependências

- Depende de: tasks 011 a 017.
- Motivo: a documentação descreve o sistema completo.

Bloqueia: entrega final da disciplina.

## Onde implementar

- `README.md` — visão geral e como executar.
- `docs/execucao.md` — passo a passo detalhado e cenários.
- `docs/biblioteca.md` — API da biblioteca cliente e exemplos.
- `docs/diagramas/` — atualizar se a arquitetura mudou (por exemplo, o
  mecanismo de endereço virtual).
- `AGENTS.md` — atualizar a seção "Estado atual".

## Como implementar

- Seguir o próprio passo a passo num clone limpo para validar.
- Exemplos da biblioteca como funções `Example` testáveis quando possível.
- Registrar nos documentos as decisões tomadas durante as tasks (pontos em
  aberto da seção 11.1 resolvidos e novas `DP-*`).

## Pontos a alinhar antes de implementar

1. **Nível mínimo de documentação da API:** ponto em aberto 11.1 nº 8.

## Critérios de aceitação

- [ ] Outra pessoa consegue compilar e executar o sistema seguindo apenas o
      README e os documentos.
- [ ] Cada cenário de teste tem instruções de execução e de leitura dos
      resultados.
- [ ] A biblioteca cliente tem documentação de API e exemplos de uso.
- [ ] Documentos e diagramas refletem a implementação final.
- [ ] `./scripts/dev.sh check` e `./scripts/dev.sh test` passam.

## Requisitos relacionados

- RF-63.
- RNF-18, RNF-19.

## Sugestão de implementação

- Validar os comandos do README num diretório temporário antes de concluir.
- Manter o README curto e levar os detalhes para `docs/execucao.md`.
