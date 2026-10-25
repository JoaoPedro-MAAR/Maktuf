Relatório: Projeto Middleware Publish/Subscribe   
Disciplina: Programação Distribuída  
Alunos: João Pedro Marques Araújo Rodrigues Aragão e Lucas Emanuel Araújo Toscano

# Etapa 0 \-  Reflexão do Projeto Maktuf

O cenário escolhido para o projeto foi o cenário A \- Telemetria de Sensores Remotos

Requisitos do Cenário A:

- Lidar com alta confiabilidade de mensagem.  
- Alta disponibilidade  
- Atraso de outros sensores não devem afetar os demais  
- Lidar bem com atemporalidade

Restrições do Cenário A:

- Baixo volume  
- Latência Relaxada  
- Dispositivos com conexão intermitente, podendo ficar sem comunicação por um período de tempo. 

Os dispositivos podem passar horas offline, 

Trabalho Relacionado:   
[A Community-Based Event Delivery Protocol in Publish/Subscribe Systems for Delay Tolerant Sensor Networks](https://www.mdpi.com/1424-8220/9/10/7580) 

[https\://sol.sbc.org.br/index.php/sbcars/article/view/30227](https://sol.sbc.org.br/index.php/sbcars/article/view/30227) 

# Etapa 1 \- Documento do Projeto 

## Componentes do sistema:

- Sensor Publicador \- Produz mensagem com as leituras de temperatura e publica nos tópicos.  
- Serviço Consumidor \- Consome e utiliza as mensagens.  
- Broker \- Recebe as mensagens, gerencia os tópicos e encaminha as mensagens.  
- Biblioteca de Comunicação \- Oferece funções para conexão, publicar mensagens, se inscrever em tópicos, cancelar inscrições.


## Restrições que afetam o componente:

- Sensor offline: Análise de uso de buffers, ou parecido com o DTN onde tem Armazenamento e Leva para frente a mensagem se tiver sensores próximos aos outros  
- Sensor offline: o sensor guarda suas leituras em um buffer persistente. Se não alcançar o broker, pode repassá-las a um sensor vizinho que tenha conexão (store-and-forward, inspirado em DTN).  
- Os sensores quando vão ficar offline para guardar energia fazem uso de uma mensagem avisando quanto tempo eles vão ficar desligados  
- Para que um sensor lento ou com muitas mensagens acumuladas não prejudique os demais, o broker mantém uma fila separada e com tamanho limitado para cada sensor, e processa as conexões de forma assíncrona (cada sensor é atendido de forma independente). 

## Estrutura da mensagem e QOS

- As mensagens são publicadas em tópicos por sensor, o que desacopla sensores e consumidores e facilita a ordenação por sensor.   
- As mensagens teriam a garantia de entrega de "**Pelo Menos uma Mensagem**"   
- As mensagens teriam **ordenação particionada** pelo Sensor  
- JSON, contando com as informações sobre o tópico, idade da leitura, número de sequência e a temperatura. Em uma mensagem que está atrasada poderia ser avaliada a seguinte mensagem seria uma lista com essa estrutura. Porém o envio seria usando uma binarização  
- Broker duplicado em caso de falha no primário o segundo assume

## Protocolos

- TCP para conexão  
- Comunicação direta entre os sensores podendo ser feito por UDP  
- Protocolo customizado parecido com DTN para envio de mensagens quando os sensores não conseguem atingir o broker  
- Desenvolvimento de um protocolo de aplicação próprio baseado no MQTT

## Tradeoffs

- Entrega "pelo menos uma vez": Garante que nehuma leitura se perde, porem a mesma mensagem chega mais de uma vez, os consumidores precisam de uma lógica implementada para evitar as duplicatas  
- Ordenação particionada por sensor: As leituras de cada sensor chegam em ordem, sem que um sensor precise esperar os outros para a ordenação total. Em troca o processamento fica no broker para manter a ordem de cada sensor   
- TCP entre sensor e broker: Garante uma entrega confiável e ordenada na conexão, mas em compensação gasta mais energia para cada entrega  
- UDP entre sensores: é mais leve e gasta menos energia que TCP, mas não garante entrega então o nosso protocolo precisa das confirmações próprias  
- Idade de leitura em vez de hora absoluta: os sensores não precisam de relógio sincronizado, mas quem repassa a mensagem precisa somar tempo que a guardou  
- Buffer no sensor e repasse inspirado em DTN: Leituras não devem se perder durante as horas offline e vão poder sair sem conexão direta. Em contrapartida a implementação fica mais complexa e o sensor gasta mais bateria e memoria  
- Repasse via sensor: sensores sem conexão podem passar para outros sensores, mas isso gasta recurso do sensor ponte.  
- Fila separada e limitada por sensor no broker: Um sensor lento ou com muitas mensagens acumuladas não vai atrasar os outros devido a uma fila separada para cada sensor  
- Broker Replicado: O sistema precisa de alta disponibilidade então é feita a duplicação do broker para caso este falhe outro pode assumir, porém fica mais complexo de gerenciar os estados entre n brokers

## Arquitetura

### Diagrama de Sequência 

#### Rascunho

![Rascunho do diagrama de sequência](rascunho_sequencia.png)

#### Diagrama completo realizado com ajuda de IA

![Diagrama de sequência](diagrama_sequencia.png)

### Diagrama de Componentes 

#### Rascunho

![Rascunho do diagrama de componentes](rascunho_componentes.png)

#### Definitivo com ajuda de IA 

![Diagrama de componentes](arquitetura_componentes_v2.png)

