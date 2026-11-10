# Maktuf

O **Maktuf** é um middleware Publish/Subscribe desenvolvido em Go para o cenário de **telemetria de sensores remotos**. Ele permite que sensores de temperatura com conexão intermitente publiquem leituras em tópicos e que serviços consumidores se inscrevam para recebê-las.

O projeto prioriza alta confiabilidade por mensagem, alta disponibilidade e isolamento entre sensores. Para isso, prevê buffer persistente nos sensores, entrega com garantia de pelo menos uma vez, ordenação por sensor, broker replicado com failover e uma biblioteca cliente que esconde a reconexão e a troca de broker.

## Documentação

- [Requisitos e definição do projeto](docs/requisitos/Etapa%200%20e%201.md)
- [Requisitos detalhados](docs/requisitos/requisitos.md)
- [Diagramas (PNG e PlantUML)](docs/diagramas/)
