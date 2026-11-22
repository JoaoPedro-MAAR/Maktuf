# Maktuf

O **Maktuf** é um middleware Publish/Subscribe desenvolvido em Go para o cenário de **telemetria de sensores remotos**. Ele permite que sensores de temperatura com conexão intermitente publiquem leituras em tópicos e que serviços consumidores se inscrevam para recebê-las.

O projeto prioriza alta confiabilidade por mensagem, alta disponibilidade e isolamento entre sensores:
- buffer persistente nos sensores;
- entrega com garantia de pelo menos uma vez e ordem por sensor;
- protocolo binário próprio sobre TCP;
- broker replicado com failover coordenado por um Juiz;
- uma biblioteca cliente que esconde a reconexão e a troca de broker atrás de um único endereço virtual.

## Componentes

| Diretório | O que é |
|---|---|
| `cmd/broker` | Broker (primário ou reserva), com filas limitadas por sensor e por assinante e replicação síncrona |
| `cmd/juiz` | Juiz: heartbeat, detecção de falha e promoção da reserva |
| `cmd/vip` | Endereço virtual: leva cada cliente ao primário atual |
| `cmd/sensor`, `cmd/consumidor` | Simulador de sensores e consumidor de exemplo |
| `cmd/sensor-manual`, `cmd/consumidor-manual` | Um sensor e um consumidor controlados pelo teclado, para demonstrações |
| `cmd/painel` | Aplicação de exemplo com página web: sensores e consumidor comandados pelo navegador, brokers observados |
| `internal/client` | Biblioteca cliente |
| `examples/` | Gerador de carga e falhas e cenários de medição |

## Início rápido

Requer Go 1.22.2 ou superior.

```bash
go build -o bin/ ./cmd/...
./scripts/dev.sh test
go run ./examples/runner -cenario examples/cenarios/queda_primario.json
```

O último comando sobe o sistema completo, derruba o primário com `kill -9` e
mostra se o failover cumpriu as metas (RTO ≤ 30 s, nenhuma leitura perdida).
Para subir os processos na mão e provocar falhas, veja
[docs/execucao.md](docs/execucao.md).

## Documentação

- [Execução](docs/execucao.md): compilar, subir o ambiente completo, provocar falhas, rodar cenários e ler métricas
- [Biblioteca cliente](docs/biblioteca.md): API e exemplos para sensores e consumidores
- [Roteiro de apresentação](docs/teste.md): demonstração ao vivo, passo a passo
- [Cenários de carga e falhas](examples/README.md): formato e leitura dos resultados
- [Métricas](docs/metricas.md)
- [Protocolo de aplicação](docs/protocolo.md)
- [Ressalvas](docs/ressalvas.md): limitações conhecidas e decisões com custo
- [Requisitos e definição do projeto](docs/requisitos/Etapa%200%20e%201.md)
- [Requisitos detalhados](docs/requisitos/requisitos.md)
- [Diagramas (PNG e PlantUML)](docs/diagramas/)
- [Tasks de implementação](docs/tasks/)
