# Requisitos do Middleware Maktuf

**Projeto:** Middleware Publish/Subscribe para Telemetria de Sensores Remotos  
**Cenário escolhido:** Cenário A  
**Linguagem de implementação:** Go  

Este documento detalha os requisitos do projeto a partir do documento base da disciplina e do documento [Etapa 0 e 1](Etapa%200%20e%201.md). Ele separa o que é **requisito obrigatório**, o que é **decisão de projeto** e o que ainda precisa ser **validado**.

---

## 1. Contexto e escopo

### 1.1 Problema

Biólogos marinhos instalam sensores de baixo consumo para monitorar a temperatura da água. Os sensores se conectam de forma intermitente: podem ficar horas offline por economia de bateria ou perda de sinal. Alguns enviam poucas leituras por dia; outros produzem um fluxo contínuo. O serviço de coleta não pode ficar indisponível quando um sensor falha ou desaparece, e um sensor lento ou com má conexão não pode atrasar a entrega dos dados dos demais.

O sistema deve ser um middleware de troca de mensagens baseado em Publish/Subscribe, com um broker intermediário e uma biblioteca cliente utilizável pelas aplicações.

### 1.2 Restrições do cenário A

| Restrição | Implicação no projeto |
|---|---|
| Baixo volume | O sistema não precisa ser dimensionado para altíssimas taxas, mas deve suportar rajadas e picos localizados. |
| Latência relaxada | A confiabilidade e a disponibilidade têm prioridade sobre a latência. Atrasos de segundos são aceitáveis. |
| Alta confiabilidade por mensagem | Leituras confirmadas não podem ser perdidas. A garantia adotada é **pelo menos uma vez** (at-least-once). |
| Conexão intermitente | Sensores precisam de buffer persistente e reenvio de leituras após reconexão. |
| Alta disponibilidade | O serviço deve continuar operando quando um sensor falha ou quando o broker primário cai. |
| Justiça entre cargas heterogêneas | Um sensor lento, agressivo ou com muitas mensagens acumuladas não pode degradar os demais. |
| Atemporalidade | As leituras podem ser antigas quando chegam ao broker; o sistema deve lidar com idades de leitura sem exigir relógios sincronizados. |

### 1.3 Objetivo do sistema

Fornecer um middleware que permita:

1. Sensores publicarem leituras de temperatura em tópicos.
2. Consumidores se inscreverem em tópicos e receberem as leituras.
3. O broker encaminhar mensagens, gerenciar tópicos, armazenar dados e coordenar a réplica.
4. A biblioteca cliente esconder detalhes de conexão, reconexão, codificação binária e failover.
5. As aplicações de teste gerarem carga realista e métricas verificáveis.

### 1.4 Escopo incluído

- Middleware Pub/Sub com broker primário, broker reserva e coordenador (Juiz).
- Biblioteca de comunicação para sensores e consumidores.
- Protocolo de aplicação próprio sobre TCP, inspirado no MQTT.
- Buffer persistente no sensor para operação offline.
- Replicação do broker e failover transparente para os clientes.
- Aplicações de exemplo: simulador de sensores, consumidor e gerador de carga.
- Coleta de métricas para a Etapa 3.

### 1.5 Fora de escopo

- Interface gráfica.
- Autenticação e criptografia ponta a ponta.
- Persistência de longo prazo para análise histórica.
- Garantia de entrega exatamente uma vez.
- Ordenação global entre sensores diferentes.
- Escalabilidade horizontal para dezenas de brokers.
- Compatibilidade com clientes MQTT reais.

### 1.6 Premissas

- Cada sensor possui um identificador único.
- Cada sensor mantém um contador de sequência monotônico por sessão de leitura.
- Os sensores não possuem relógio sincronizado; por isso enviam a **idade** da leitura.
- O broker pode calcular o instante da leitura como `agora - idade`.
- Consumidores devem ser idempotentes ou usar a biblioteca para descartar duplicatas.
- A biblioteca cliente conhece apenas o endereço virtual do broker, nunca a instância física.
- O número inicial de brokers é 2 (um primário e um reserva) mais 1 Juiz.
- O tráfego de dados é majoritariamente de baixo volume, com possibilidade de rajadas.

---

## 2. Atores e componentes

| Ator / componente | Responsabilidade |
|---|---|
| Sensor publicador | Produz leituras, mantém buffer persistente e publica no broker. |
| Serviço consumidor | Inscreve-se em tópicos, recebe leituras e trata duplicatas. |
| Broker primário | Recebe conexões, gerencia tópicos, persiste mensagens, encaminha para consumidores e replica estado. |
| Broker reserva | Mantém cópia das leituras e inscrições; assume no failover. |
| Juiz | Monitora heartbeats, coordena failover e sincronização entre brokers. Não recebe leituras. |
| Biblioteca de comunicação | Fornece API de conexão, publicação, inscrição, cancelamento, reconexão e codificação binária. |
| Aplicações de exemplo | Simulam sensores, consumidores e cargas para validar os requisitos. |

---

## 3. Glossário

| Termo | Definição |
|---|---|
| Tópico | Canal lógico identificado por uma string, usado para classificar mensagens. |
| Inscrição | Registro de interesse de um cliente em um tópico ou filtro de tópicos. |
| Tópico ativo | Tópico com pelo menos um inscrito no momento. |
| Leitura | Mensagem de telemetria contendo sensor, sequência, idade e temperatura. |
| `sensor_id` | Identificador único do sensor. |
| `seq` | Número de sequência monotônico por sensor, usado para ordenação e deduplicação. |
| ACK(seq) | Confirmação do broker de que a leitura com aquela sequência foi persistida e replicada. |
| NO_SUBSCRIBERS | Resposta do broker informando que a mensagem foi descartada por não haver inscritos. |
| At-least-once | Garantia de que uma mensagem confirmada será entregue ao menos uma vez; duplicatas são possíveis. |
| Ordenação particionada | Mensagens de um mesmo sensor são entregues em ordem de `seq`; não há ordem global entre sensores. |
| Idade da leitura | Tempo decorrido desde a coleta até o envio, usado no lugar de timestamp absoluto. |
| DORMINDO | Estado de um sensor que avisou que ficará desligado por um período. |
| OFFLINE | Estado de um sensor que não cumpriu o prazo de sono ou perdeu conexão. |
| Failover | Promoção do broker reserva para primário quando o primário falha. |
| IP virtual | Endereço único conhecido pelos clientes, que aponta sempre para o broker ativo. |
| Perfil de referência | Carga de teste usada para validar os requisitos de desempenho. |

---

## 4. Requisitos funcionais

A prioridade segue a escala:

- **Essencial:** obrigatório para a entrega.
- **Alta:** necessário para o cenário.
- **Média:** desejável, desde que não comprometa os essenciais.

### 4.1 Tópicos, publicação e inscrição

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-01 | Essencial | Toda mensagem deve estar associada a um tópico. | O broker rejeita ou descarta qualquer mensagem sem tópico válido. |
| RF-02 | Essencial | Um cliente deve poder publicar mensagens em um tópico. | Um publicador conectado consegue enviar uma leitura e receber uma resposta do broker. |
| RF-03 | Essencial | Um cliente deve poder se inscrever em um tópico. | Após a inscrição, o cliente recebe mensagens publicadas naquele tópico. |
| RF-04 | Essencial | Um cliente deve poder remover sua inscrição. | Após o cancelamento, o cliente deixa de receber novas mensagens do tópico. |
| RF-05 | Essencial | Tópicos devem ter ciclo de vida dinâmico. | Um tópico é criado na primeira inscrição e removido quando o último inscrito cancela. |
| RF-06 | Essencial | O tópico deve existir apenas enquanto houver inscritos. | Não há replay de mensagens antigas para um novo inscrito; publicações feitas antes da inscrição não são entregues depois. |
| RF-07 | Essencial | Publicação em tópico sem inscritos deve ser descartada, com ciência do publicador. | O broker responde `NO_SUBSCRIBERS`; o sensor remove a leitura do buffer e registra o descarte. |
| RF-08 | Essencial | Publicação em tópico com inscritos deve ser encaminhada a todos e então apagada. | Todos os inscritos ativos recebem a mensagem; após o enfileiramento para todos, a cópia original é removida conforme a política de retenção. |
| RF-09 | Alta | Deve haver suporte a filtros de tópicos com curingas. | Uma inscrição em `sensores/+/temperatura` recebe leituras de qualquer sensor. |
| RF-10 | Alta | O cancelamento de inscrição deve interromper entregas futuras e descartar mensagens ainda enfileiradas para aquele inscrito. | O consumidor não recebe mensagens após o `UNSUBSCRIBE`; os descartes são contabilizados. |
| RF-11 | Média | Publicações simultâneas de vários sensores não devem ser bloqueadas por um único tópico. | O broker processa publicações concorrentes sem serialização global. |

### 4.2 Entrega, confiabilidade e ordenação

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-12 | Essencial | A garantia de entrega adotada é **pelo menos uma vez**. | Uma leitura confirmada com ACK é entregue ao menos uma vez, mesmo com falhas de conexão. |
| RF-13 | Essencial | O ACK(seq) só pode ser enviado após a leitura ser persistida no broker primário e confirmada pela réplica. | Um ACK recebido pelo sensor implica que a leitura sobrevive à queda do broker primário. |
| RF-14 | Essencial | Duplicatas são permitidas e devem ser tratáveis no consumidor. | Consumidores descartam mensagens repetidas usando `sensor_id + seq`. |
| RF-15 | Essencial | As mensagens devem ter ordenação particionada por sensor. | Para um mesmo sensor, o consumidor recebe `seq` em ordem crescente, mesmo após reconexão. |
| RF-16 | Essencial | O broker deve manter um cursor de entrega por assinante. | Após reconexão, o broker retoma do ponto pendente sem perder mensagens confirmadas. |
| RF-17 | Alta | Mensagens devem ser codificadas em formato binário compacto. | O protocolo define framing binário e não utiliza JSON/XML no transporte. |
| RF-18 | Essencial | A mensagem de leitura deve conter `sensor_id`, `seq`, `age_ms` e `temperature`. | O consumidor consegue identificar sensor, ordem, idade e valor da leitura. |
| RF-19 | Essencial | O broker deve calcular o instante da leitura como `agora - age_ms`. | Sensores sem relógio sincronizado produzem leituras com timestamp coerente no broker. |
| RF-20 | Alta | Leituras antigas devem ser aceitas e marcadas com sua idade original. | Uma leitura com idade de horas é aceita após reconexão e entregue com o timestamp calculado. |
| RF-21 | Alta | O sistema deve publicar mudanças de estado dos sensores em tópico próprio. | Consumidores inscritos em `sensores/{id}/status` recebem transições `online`, `dormindo` e `offline`. |
| RF-22 | Média | O consumidor deve poder identificar lacunas de sequência. | Ao detectar salto em `seq`, o consumidor registra a lacuna ou solicita recuperação conforme a política definida. |

### 4.3 Sensores intermitentes, buffer e estados

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-23 | Essencial | O sensor deve manter buffer persistente das leituras ainda não confirmadas. | Leituras sobrevivem a reinício do processo do sensor. |
| RF-24 | Essencial | O sensor só pode remover uma leitura do buffer após resposta definitiva do broker. | ACK(seq) ou `NO_SUBSCRIBERS` removem a leitura; falha de conexão não remove. |
| RF-25 | Essencial | Ao reconectar, o sensor deve reenviar as leituras pendentes da mais antiga para a mais nova. | A ordem de reenvio respeita `seq` crescente por sensor. |
| RF-26 | Essencial | Quando o buffer encher, o sensor deve descartar a leitura mais antiga. | O descarte é registrado com contador e sequência para permitir detecção de lacuna. |
| RF-27 | Alta | O sensor deve informar ao broker quando entrar em modo de economia de energia. | O broker recebe `SLEEP(duração)` e marca o sensor como DORMINDO. |
| RF-28 | Alta | O broker deve marcar o sensor como OFFLINE quando o prazo de sono expirar sem contato. | Após `duração + tolerância`, o tópico de status recebe `offline`. |
| RF-29 | Alta | Ao retomar contato, o sensor deve ser marcado como online. | O tópico de status recebe `online` após nova conexão válida. |
| RF-30 | Alta | Falha de conexão antes do ACK deve manter a leitura no buffer para retry. | A biblioteca reconecta e reenvia; duplicatas são possíveis e aceitas. |
| RF-31 | Média | O sensor deve reportar perdas por overflow no buffer. | O broker ou o consumidor consegue distinguir lacuna por descarte de lacuna por falha. |

### 4.4 Isolamento, buffering e justiça

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-32 | Essencial | Cada sensor deve ter sessão e fila de processamento independentes no broker. | Um sensor com backlog não impede o atendimento de outro. |
| RF-33 | Essencial | O broker deve processar conexões de forma assíncrona. | Uma conexão lenta não bloqueia o event loop de atendimento dos demais. |
| RF-34 | Essencial | A fila por sensor deve ter tamanho limitado e política explícita ao encher. | Ao encher, o broker aplica backpressure apenas àquele sensor; os outros continuam. |
| RF-35 | Essencial | Rajadas de mensagens em um tópico devem ser bufferizadas. | O broker aceita uma rajada sem recusar novas conexões e entrega após o pico. |
| RF-36 | Alta | Cada assinante deve ter fila de entrega independente e limitada. | Um consumidor lento não bloqueia a entrega para os demais. |
| RF-37 | Alta | Não pode haver perda silenciosa de mensagem já confirmada. | Se a retenção de um assinante for excedida, o sistema gera evidência explícita de lacuna. |
| RF-38 | Alta | O escalonamento entre filas deve ser justo. | Um sensor agressivo não eleva a latência dos sensores bem-comportados acima do limite aceitável. |
| RF-39 | Média | O tamanho das filas e o tempo de retenção devem ser configuráveis. | Os limites podem ser ajustados por configuração sem recompilar o sistema. |

### 4.5 Disponibilidade, replicação e failover

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-40 | Essencial | Deve existir um broker primário e um broker reserva sincronizado. | A reserva possui as leituras e inscrições necessárias para assumir. |
| RF-41 | Essencial | Leituras e inscrições devem ser replicadas antes do ACK ao sensor. | O sensor só recebe ACK após a confirmação da réplica. |
| RF-42 | Essencial | O Juiz deve monitorar os brokers por heartbeat. | A ausência de heartbeat dentro do prazo dispara avaliação de failover. |
| RF-43 | Essencial | Em caso de falha do primário, o Juiz deve promover a reserva. | A reserva assume o IP virtual e passa a aceitar conexões. |
| RF-44 | Essencial | O failover deve ser transparente para as aplicações. | A biblioteca reconecta sozinha; a aplicação não precisa reiniciar nem refazer `SUBSCRIBE`. |
| RF-45 | Essencial | A troca de broker não pode perder leituras já confirmadas. | RPO igual a zero para mensagens com ACK. |
| RF-46 | Essencial | A falha de um sensor não pode indisponibilizar o serviço. | Os demais sensores continuam publicando e os consumidores continuam recebendo. |
| RF-47 | Alta | Quando o antigo primário voltar, ele deve entrar como reserva e ressincronizar. | O Juiz coordena a sincronização sem interromper o broker ativo. |
| RF-48 | Alta | A falha do Juiz não pode interromper o fluxo de dados. | Brokers continuam atendendo; o failover fica suspenso até o Juiz retornar. |
| RF-49 | Alta | Múltiplas instâncias de broker devem ser transparentes para a biblioteca cliente. | O cliente usa apenas o endereço virtual e não conhece a topologia interna. |
| RF-50 | Média | O sistema deve suportar reinício do broker sem perda de estado confirmado. | Após reinício, leituras persistidas e inscrições são recuperadas. |

### 4.6 Biblioteca de comunicação

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-51 | Essencial | A biblioteca deve oferecer operações de conectar, desconectar, publicar, inscrever e cancelar inscrição. | As aplicações de exemplo usam apenas essas operações. |
| RF-52 | Essencial | A publicação deve retornar o resultado do broker. | A aplicação distingue `PERSISTED`, `NO_SUBSCRIBERS` e erro temporário. |
| RF-53 | Essencial | A biblioteca deve reconectar automaticamente com backoff. | Uma queda de conexão não exige ação da aplicação. |
| RF-54 | Essencial | A biblioteca deve restaurar as inscrições após reconexão. | O consumidor continua recebendo mensagens sem novo `SUBSCRIBE` explícito. |
| RF-55 | Alta | A biblioteca deve permitir o uso concorrente por múltiplas goroutines. | Publicações e inscrições simultâneas não corrompem o estado da conexão. |
| RF-56 | Alta | A biblioteca deve expor callbacks ou canais para entrega de mensagens e eventos de estado. | Sensores e consumidores são notificados de mensagens, reconexão e erro. |
| RF-57 | Média | A biblioteca deve oferecer suporte à deduplicação por `sensor_id + seq`. | O consumidor pode ativar a deduplicação sem implementar a lógica do zero. |
| RF-58 | Média | Timeouts e limites devem ser configuráveis. | Conexão, heartbeat, retry e buffering podem ser ajustados. |

### 4.7 Aplicações de exemplo e métricas

| ID | Prioridade | Requisito | Critério de aceitação |
|---|---|---|---|
| RF-59 | Essencial | Deve existir um simulador de múltiplos sensores. | É possível configurar quantidade, taxa, falhas, sono e reconexão. |
| RF-60 | Essencial | Deve existir um serviço consumidor de exemplo. | O consumidor se inscreve, recebe, deduplica e registra métricas. |
| RF-61 | Essencial | Deve existir um gerador de carga e falhas. | É possível reproduzir rajadas, sensores lentos e queda do broker. |
| RF-62 | Essencial | As aplicações devem coletar métricas de desempenho. | Latência, vazão, duplicatas, perdas, backlog e failover são registrados. |
| RF-63 | Alta | Deve haver documentação para executar os cenários de teste. | Outra pessoa consegue reproduzir os testes seguindo o README/documentação. |

---

## 5. Requisitos não funcionais

### 5.1 Desempenho

| ID | Requisito | Meta inicial |
|---|---|---|
| RNF-01 | Latência fim a fim em operação normal | p95 ≤ 2 s e p99 ≤ 10 s para leituras online. |
| RNF-02 | Latência durante recuperação de sensor offline | Atraso de recuperação aceitável; não conta para a meta de latência online. |
| RNF-03 | Vazão do perfil de referência | Suportar pelo menos 200 sensores, sendo 90% de baixa taxa (1 leitura a cada 5 min) e 10% de taxa contínua (1 leitura/s), com rajadas de até 10x. |
| RNF-04 | Rajadas | Absorver rajada de 2.000 mensagens sem recusar novas conexões e sem perda de mensagens confirmadas. |
| RNF-05 | Justiça | A latência p95 de um sensor bem-comportado não pode ultrapassar 2x a linha de base quando outro sensor estiver saturado. |
| RNF-06 | Failover | RTO ≤ 30 s para o broker reserva assumir e clientes reconectarem. |
| RNF-07 | Perda de dados | RPO = 0 para leituras confirmadas com ACK. |
| RNF-08 | Disponibilidade | Alvo de 99,9% de disponibilidade do serviço durante os testes, desconsiderando falhas programadas. |

As metas acima são **propostas iniciais** e devem ser ajustadas após a Etapa 2 e validadas na Etapa 3.

### 5.2 Confiabilidade e durabilidade

| ID | Requisito |
|---|---|
| RNF-09 | Nenhuma leitura confirmada pode ser perdida por falha de broker, desde que a réplica esteja sincronizada. |
| RNF-10 | Duplicatas são aceitáveis, desde que detectáveis e descartáveis pelo consumidor. |
| RNF-11 | A ordenação por sensor deve ser preservada mesmo após reconexão e failover. |
| RNF-12 | O buffer do sensor e o armazenamento do broker devem sobreviver a reinício de processo. |
| RNF-13 | O sistema deve distinguir falha temporária, descarte por ausência de inscritos e perda por overflow. |

### 5.3 Eficiência de recursos

| ID | Requisito |
|---|---|
| RNF-14 | O protocolo deve usar codificação binária para reduzir bytes por leitura. |
| RNF-15 | O sensor deve evitar polling agressivo e usar conexões intermitentes com reconexão sob demanda. |
| RNF-16 | O broker não deve manter cópias ilimitadas de uma mesma mensagem em memória. |
| RNF-17 | O consumo de memória deve ser limitado por filas configuráveis. |

### 5.4 Manutenibilidade, testabilidade e operação

| ID | Requisito |
|---|---|
| RNF-18 | O código deve ser organizado em pacotes Go com responsabilidades claras. |
| RNF-19 | A biblioteca cliente deve ter documentação de API e exemplos de uso. |
| RNF-20 | O sistema deve ter testes automatizados para protocolo, buffer, deduplicação, ordenação e failover. |
| RNF-21 | As métricas devem ser expostas de forma padronizada, preferencialmente em formato textual ou Prometheus. |
| RNF-22 | Os cenários de teste devem ser reproduzíveis por configuração. |
| RNF-23 | O sistema deve registrar logs estruturados com identificação de sensor, tópico e sequência. |

### 5.5 Segurança e limitações

| ID | Requisito |
|---|---|
| RNF-24 | Autenticação, autorização e criptografia não fazem parte do escopo inicial. |
| RNF-25 | O sistema deve assumir que os identificadores de sensor e consumidor são confiáveis no ambiente de teste. |
| RNF-26 | Qualquer uso fora do ambiente controlado deve ser precedido de autenticação e TLS. |

---

## 6. Requisitos do protocolo de aplicação

O protocolo é próprio, binário e inspirado no MQTT, transportado sobre TCP.

### 6.1 Formato geral

Cada mensagem deve possuir:

| Campo | Descrição |
|---|---|
| `length` | Tamanho do restante da mensagem. |
| `version` | Versão do protocolo. |
| `type` | Tipo da mensagem. |
| `flags` | Indicadores de controle. |
| `payload` | Corpo binário específico do tipo. |

### 6.2 Tipos de mensagem

| Tipo | Direção | Finalidade |
|---|---|---|
| `CONNECT` | Cliente → Broker | Abrir sessão com identificador e papel. |
| `CONNACK` | Broker → Cliente | Confirmar conexão. |
| `PUBLISH` | Cliente → Broker | Publicar leitura ou status. |
| `PUBACK` | Broker → Cliente | Confirmar persistência ou informar descarte. |
| `SUBSCRIBE` | Cliente → Broker | Inscrever-se em tópico ou filtro. |
| `SUBACK` | Broker → Cliente | Confirmar inscrição. |
| `UNSUBSCRIBE` | Cliente → Broker | Remover inscrição. |
| `UNSUBACK` | Broker → Cliente | Confirmar remoção. |
| `DELIVER` | Broker → Cliente | Entregar mensagem ao consumidor. |
| `DELIVER_ACK` | Cliente → Broker | Confirmar recebimento da entrega. |
| `SLEEP` | Sensor → Broker | Informar período de sono. |
| `HEARTBEAT` | Broker → Juiz | Informar que a instância está viva. |
| `REPLICA` | Primário → Reserva | Replicar leitura ou inscrição. |
| `REPLICA_ACK` | Reserva → Primário | Confirmar replicação. |
| `PROMOTE` | Juiz → Reserva | Promover reserva a primário. |

### 6.3 Semântica de confirmação

| Situação | Resposta | Efeito no sensor |
|---|---|---|
| Leitura persistida e replicada | `PUBACK(PERSISTED, seq)` | Remove do buffer. |
| Tópico sem inscritos | `PUBACK(NO_SUBSCRIBERS, seq)` | Remove do buffer e registra descarte. |
| Falha temporária | Erro ou ausência de resposta | Mantém no buffer e tenta novamente. |
| Duplicata recebida | `PUBACK(PERSISTED, seq)` ou resposta idempotente | Remove do buffer. |

### 6.4 Regras de entrega

- O broker só entrega mensagens para inscritos ativos no momento da publicação.
- Não há replay histórico para novos inscritos.
- Mensagens de um mesmo sensor são entregues em ordem crescente de `seq`.
- O consumidor deve confirmar entregas para permitir o avanço do cursor.
- Mensagens entregues e confirmadas por todos os inscritos podem ser removidas do armazenamento conforme a política de retenção.

---

## 7. Casos de borda e comportamento esperado

| ID | Caso | Comportamento esperado |
|---|---|---|
| CE-01 | Sensor publica sem inscritos | Mensagem descartada; sensor recebe `NO_SUBSCRIBERS`; leitura sai do buffer. |
| CE-02 | Sensor cai antes do ACK | Leitura permanece no buffer; após reconexão é reenviada; duplicata é possível. |
| CE-03 | Sensor fica offline por horas | Leituras são acumuladas no buffer; ao reconectar são enviadas da mais antiga para a mais nova. |
| CE-04 | Buffer do sensor enche | Leitura mais antiga é descartada; contador e sequência são registrados. |
| CE-05 | Sensor anuncia sono e não retorna | Broker marca OFFLINE após o prazo e publica no tópico de status. |
| CE-06 | Sensor anuncia sono e retorna dentro do prazo | Broker marca ONLINE e aceita o backlog. |
| CE-07 | Broker primário falha durante publicação | Sensor não recebe ACK; reserva assume; sensor reenvia e pode gerar duplicata. |
| CE-08 | Broker primário falha com leitura já confirmada | Reserva possui a leitura; nenhuma perda ocorre. |
| CE-09 | Juiz falha | Fluxo de dados continua; failover fica suspenso até o Juiz retornar. |
| CE-10 | Consumidor lento | Sua fila cresce até o limite; demais consumidores não são afetados. |
| CE-11 | Consumidor ultrapassa a retenção | Lacuna é explicitada; o consumidor deve ser capaz de detectar e registrar. |
| CE-12 | Último inscrito cancela | Tópico é removido; mensagens enfileiradas para ele são descartadas. |
| CE-13 | Novo inscrito em tópico recém-criado | Recebe apenas mensagens publicadas após a inscrição. |
| CE-14 | Sensor envia leitura com idade muito alta | Broker aceita, calcula timestamp e entrega com a idade original. |
| CE-15 | Mensagem duplicada chega ao consumidor | Consumidor descarta com base em `sensor_id + seq`. |
| CE-16 | Mensagem chega fora de ordem | Broker e consumidor garantem ordenação por sensor; mensagens antigas já processadas são descartadas. |
| CE-17 | Duas instâncias de broker no ar | Cliente continua usando o endereço virtual; apenas o primário atende. |
| CE-18 | Antigo primário retorna | Entra como reserva e sincroniza antes de poder assumir. |

---

## 8. Critérios de aceitação e testes

| ID | Teste | Requisitos cobertos |
|---|---|---|
| CT-01 | Publicar em tópico sem inscritos e verificar `NO_SUBSCRIBERS`. | RF-07 |
| CT-02 | Publicar com múltiplos inscritos e verificar entrega a todos. | RF-08 |
| CT-03 | Inscrever, cancelar e verificar que não há mais entregas. | RF-03, RF-04, RF-10 |
| CT-04 | Criar e remover tópico dinamicamente; verificar que não há replay. | RF-05, RF-06, CE-13 |
| CT-05 | Simular sensor offline, reconectar e verificar reenvio ordenado. | RF-23, RF-24, RF-25 |
| CT-06 | Testar sono dentro e fora do prazo. | RF-27, RF-28, RF-29 |
| CT-07 | Encher o buffer do sensor e verificar descarte do mais antigo. | RF-26, CE-04 |
| CT-08 | Gerar rajada e verificar buffering sem recusa de conexões. | RF-35, RNF-04 |
| CT-09 | Simular sensor lento e medir latência dos demais. | RF-32, RF-33, RF-34, RF-38, RNF-05 |
| CT-10 | Simular consumidor lento e verificar isolamento. | RF-36, RF-37, CE-10 |
| CT-11 | Derrubar broker primário e verificar failover e continuidade. | RF-40 a RF-46, RNF-06, RNF-07 |
| CT-12 | Verificar reconexão automática e restauração de inscrições. | RF-44, RF-53, RF-54 |
| CT-13 | Verificar deduplicação e ordenação por sensor. | RF-14, RF-15, RF-16 |
| CT-14 | Verificar cálculo de timestamp a partir da idade. | RF-18, RF-19, RF-20 |
| CT-15 | Derrubar o Juiz e verificar que o fluxo de dados continua. | RF-48, CE-09 |
| CT-16 | Reiniciar broker e verificar persistência de leituras e inscrições. | RF-50, RNF-12 |
| CT-17 | Medir latência p95/p99 e vazão no perfil de referência. | RNF-01, RNF-03 |
| CT-18 | Verificar métricas de perdas, duplicatas e backlog. | RF-62, RNF-13, RNF-21 |

---

## 9. Métricas e observabilidade

O sistema deve coletar, no mínimo:

| Métrica | Descrição |
|---|---|
| `messages_published_total` | Total de mensagens publicadas por sensor e tópico. |
| `messages_delivered_total` | Total de entregas realizadas. |
| `messages_duplicated_total` | Total de duplicatas detectadas. |
| `messages_dropped_no_subscribers_total` | Total de mensagens descartadas por ausência de inscritos. |
| `sensor_buffer_overflow_total` | Total de descartes por overflow no sensor. |
| `broker_queue_depth` | Profundidade das filas por sensor e por assinante. |
| `delivery_latency_seconds` | Latência entre publicação e entrega, com p50, p95 e p99. |
| `ack_latency_seconds` | Latência entre publicação e ACK. |
| `failover_duration_seconds` | Tempo entre detecção da falha e retomada do serviço. |
| `active_sensors` | Número de sensores conectados ou em estado conhecido. |
| `active_subscriptions` | Número de inscrições ativas por tópico. |
| `topic_count` | Número de tópicos ativos. |
| `retention_drops_total` | Mensagens perdidas por exceder retenção de assinante. |

Essas métricas devem ser usadas nos gráficos da Etapa 3 e para validar os RNFs.

---

## 10. Rastreabilidade com os requisitos mínimos da disciplina

| Requisito mínimo | Requisitos do projeto | Status |
|---|---|---|
| Mensagens com tópico | RF-01 | Coberto |
| Publicar, inscrever e remover inscrição | RF-02, RF-03, RF-04 | Coberto |
| Ciclo de vida dinâmico dos tópicos | RF-05, RF-06 | Coberto |
| Publicação sem inscritos descartada com ciência do publicador | RF-07, CE-01 | Coberto |
| Publicação com inscritos encaminhada a todos e apagada | RF-08, RF-10 | Coberto |
| Rajadas bufferizadas | RF-35 | Coberto |
| Múltiplos brokers transparentes para a biblioteca | RF-44, RF-49 | Coberto |
| Alta confiabilidade por mensagem | RF-12, RF-13, RNF-07, RNF-09 | Coberto |
| Alta disponibilidade | RF-40 a RF-50, RNF-06 | Coberto |
| Justiça entre cargas heterogêneas | RF-32 a RF-38, RNF-05 | Coberto |
| Atemporalidade | RF-19, RF-20 | Coberto |
| Baixo volume e latência relaxada | RNF-01 a RNF-03 | Coberto com metas propostas |
| Protocolo próprio sobre TCP | Seção 6 | Coberto |

---

## 11. Decisões de projeto e pontos a validar

As decisões abaixo foram adotadas para eliminar ambiguidades do documento base.

| ID | Tema | Decisão adotada | Justificativa |
|---|---|---|---|
| DP-01 | Criação de tópico | O tópico é criado na primeira inscrição e removido quando o último inscrito cancela. | Alinha o ciclo de vida dinâmico com a regra de descarte quando não há inscritos. |
| DP-02 | Publicação sem inscritos | A mensagem é descartada e o publicador recebe `NO_SUBSCRIBERS`. | Atende ao requisito mínimo e evita retenção indefinida. |
| DP-03 | Sem replay histórico | Novos inscritos recebem apenas mensagens futuras. | Simplifica o modelo e evita armazenamento de histórico não previsto no cenário. |
| DP-04 | ACK de leitura | O ACK significa persistência no primário e replicação na reserva. | Garante RPO zero para leituras confirmadas. |
| DP-05 | Exclusão após entrega | A cópia original é removida após o enfileiramento para todos os inscritos; cada fila mantém sua própria retenção. | Permite apagar a mensagem sem perder entregas pendentes de consumidores lentos. |
| DP-06 | Backpressure | Ao encher a fila de um sensor, o broker para de confirmar aquele sensor; os demais não são afetados. | Preserva confiabilidade e justiça. |
| DP-07 | Consumidor lento | Cada assinante tem fila limitada; ao exceder a retenção, a lacuna é explicitada. | Evita perda silenciosa e bloqueio global. |
| DP-08 | Falha do Juiz | O fluxo de dados continua; o failover fica suspenso até o Juiz retornar. | O Juiz não está no caminho crítico dos dados. |
| DP-09 | Duplicatas | São permitidas e o consumidor deve deduplicar por `sensor_id + seq`. | At-least-once é mais simples e atende à alta confiabilidade. |
| DP-10 | Relógio | O sensor envia `age_ms`; o broker calcula o timestamp. | Evita dependência de sincronização de relógios. |
| DP-11 | Codificação | Protocolo binário próprio sobre TCP. | Reduz overhead e permite controle do protocolo. |
| DP-12 | Número de brokers | 2 brokers (primário e reserva) + 1 Juiz. | Atende à disponibilidade com complexidade inicial controlada. |

### 11.1 Pontos em aberto para discussão

1. O requisito de "tópicos com ciclo de vida dinâmico" deve ser interpretado como criação apenas por inscrição ou também por publicação?
2. A exclusão da mensagem deve ocorrer após o enfileiramento para todos os inscritos ou somente após o ACK de todos os consumidores?
3. Qual é o comportamento esperado quando um consumidor fica offline além do limite de retenção: descartar, bloquear o publicador ou notificar a aplicação?
4. O tópico de status deve seguir as mesmas regras de ciclo de vida dos tópicos de leitura?
5. Qual é o limite máximo de tamanho de mensagem e de buffer por sensor?
6. O Juiz deve ser replicado para eliminar o ponto único de falha na coordenação?
7. A garantia "pelo menos uma vez" se aplica também a mensagens de status ou apenas às leituras de temperatura?
8. Qual é o nível mínimo de documentação da API da biblioteca exigido na Etapa 2?

---

## 12. Resumo executivo

O projeto Maktuf implementa um middleware Pub/Sub para o cenário A, com foco em **alta confiabilidade por mensagem**, **alta disponibilidade** e **isolamento entre sensores**. As escolhas centrais são: garantia at-least-once, ordenação particionada por sensor, buffer persistente nos sensores, filas independentes e limitadas no broker, codificação binária sobre TCP, replicação síncrona com failover transparente e uso de idade de leitura em vez de relógio sincronizado. O sistema será validado por aplicações de exemplo que simulam sensores intermitentes, rajadas, consumidores lentos e falhas de broker.
