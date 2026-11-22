# Ressalvas

Este documento registra limitações conhecidas da política de filas implementada
para a task 008 (RF-32, RF-34 a RF-39, RNF-17).

## Descarte na fila do assinante e a garantia "pelo menos uma vez"

O sensor recebe `PUBACK(PERSISTED)` depois que a leitura é gravada no broker
primário. Em seguida, a leitura é colocada na fila de cada assinante. Se a fila
de um assinante já estiver na retenção máxima, a leitura mais antiga depois da
cabeça é descartada para abrir espaço.

Isso significa que uma leitura confirmada ao sensor pode não chegar ao
assinante. A garantia efetiva passa a ser "pelo menos uma vez enquanto a
retenção do assinante não for excedida", o que tensiona a afirmação do
documento [Etapa 0 e 1](requisitos/Etapa%200%20e%201.md) de que "nenhuma leitura
se perde" (garantia de entrega "pelo menos uma vez").

A alternativa adotada no lugar da perda silenciosa é a evidência explícita:
`GAP_BEFORE` na próxima leitura do mesmo sensor, log `WARN` e contador de
descartes. O consumidor consegue detectar que houve perda, mas não recuperá-la.

## A flag não identifica quais leituras faltaram

`GAP_BEFORE` apenas afirma que houve descarte de leituras daquele sensor antes
da leitura marcada. O protocolo não informa a lista de `seq` perdidos. O
consumidor detecta a lacuna comparando o `seq` recebido com o último conhecido.

Além disso, a flag mistura causas se o sensor também tiver descartado leituras
por overflow do próprio buffer no mesmo intervalo: o broker não distingue
"descarte por retenção do assinante" de "descarte já ocorrido no sensor".

## A cabeça da fila nunca é descartada

A cabeça pode estar em trânsito aguardando `DELIVER_ACK`; removê-la quebraria a
lógica de reconexão e o reenvio stop-and-wait. Por isso a retenção preserva a
cabeça e descarta a leitura seguinte mais antiga.

Como consequência, com o consumidor offline por muito tempo, a cabeça fica
parada indefinidamente enquanto as demais leituras rodam. O limite mínimo da
fila do assinante é 2.

## Expiração por tempo não implementada

A retenção é feita apenas por tamanho. A expiração por tempo prevista no RF-39
("o tempo de retenção deve ser configurável") ainda não foi implementada.

## Estado e prazos de status não sobrevivem ao reinício

O `statusManager` mantém estado e prazo de sono apenas em memória. As mensagens
de status já publicadas e enfileiradas para assinantes são persistidas pelo
mesmo store das leituras e sobrevivem ao reinício, mas o prazo de um sensor que
estava `dormindo` é perdido.

Consequência: se o broker reiniciar enquanto um sensor dorme, o `offline` pode
não ser publicado no prazo original. O sensor será marcado `online` quando
reconectar; se não reconectar, o estado pode ficar indefinidamente sem
`offline` até um novo `SLEEP` ou contato.

## Queda sem SLEEP não marca OFFLINE

Por decisão de escopo da task 009, apenas o prazo de `SLEEP` gera `offline`.
Uma conexão que cai sem `SLEEP` mantém o último estado conhecido (`online`).
Isso diverge do glossário de `requisitos.md`, que define OFFLINE também como
sensor que perdeu conexão.

## ONLINE é republicado a cada conexão válida

Toda conexão válida de sensor publica `online`, mesmo que o estado anterior já
fosse `online`. Isso atende ao RF-29 literalmente, mas pode gerar transições
repetidas para sensores que reconectam com frequência.

## Status compartilha a retenção do assinante

As transições de status usam a mesma fila limitada das leituras. Um assinante
muito lento pode perder transições de status por retenção; nesse caso a lacuna
é sinalizada com `GAP_BEFORE`, como nas leituras, e não há recuperação do
estado intermediário perdido. O `seq` de status é gerado pelo broker em um
fluxo separado e não deve ser comparado com o `seq` de leituras do sensor.

## Replicação vai além de "leituras e inscrições"

O diagrama de componentes e o cenário 3 do diagrama de sequência do
[Etapa 0 e 1](requisitos/Etapa%200%20e%201.md) descrevem a replicação de
"leituras e inscrições". A task 011 replica o **registro completo do store**
do primário: além de leituras e inscrições, também o cursor de `DELIVER_ACK`,
os descartes por retenção, as marcas de lacuna, o último `seq` de cada sensor e
as transições de status (com o campo `status`).

Motivo: o `REPLICA` da versão 2 não levava a lista de inscritos nem o `status`
das transições; replicar só a leitura faria uma transição `offline` chegar ao
consumidor como leitura de temperatura depois do failover. Com o registro
completo, a reserva fica idêntica ao primário e o consumidor não recebe de novo
leituras que já confirmou (o documento previa duplicatas nesse caso, o que
continua permitido pelo RNF-10).

Custo: o protocolo passou para a versão 3, e cada `DELIVER_ACK` também espera
o `REPLICA_ACK` antes de liberar a próxima entrega.

## RPO = 0 só com a reserva sincronizada (modo degradado)

O DP-04, o RF-41 e o diagrama de sequência do Etapa 0 e 1 dizem que o ACK ao
sensor só sai depois da confirmação da réplica. Para que a queda da **reserva**
não pare o sistema (o que tornaria a reserva um segundo ponto de falha), o
primário segue sozinho quando a reserva cai ou estoura o timeout:

- o `PUBACK(PERSISTED)` passa a garantir apenas a gravação no disco do
  primário;
- a reserva é marcada como dessincronizada e só volta a ser considerada
  sincronizada depois de receber de novo o estado completo;
- uma reserva dessincronizada não deve ser promovida (o Juiz confere isso,
  como no cenário 3 do diagrama de sequência).

Consequência: enquanto a reserva está dessincronizada, o RPO = 0 (RNF-07) não
vale. Leituras aceitas nesse intervalo não se perdem se só o processo do
primário cair, porque estão no disco dele, mas ficam indisponíveis até o
primário voltar, e se perdem se o disco do primário for perdido.

O DP-04 e o RF-41 de `requisitos.md` foram atualizados para refletir esse
comportamento.

## Replicação síncrona serializa as gravações

Cada registro é replicado sob o mesmo lock de escrita do store, um de cada
vez. Isso garante a ordem dos registros na reserva, mas soma a ida e volta de
rede e o fsync da reserva a cada gravação, e todas as sessões passam por essa
mesma fila. Para o cenário A (baixo volume, latência relaxada) o custo é
aceitável; não há envio em lote.

## A reserva não mantém o estado em memória

A reserva grava os registros apenas no store em disco. Tópicos, filas e
cursores em memória só são montados quando ela sobe como primário. A
promoção em funcionamento (`PROMOTE`, task 012) precisa montar esse estado a
partir do store antes de aceitar clientes.


## Juiz único (task 012)

Há um único Juiz (DP-12, ponto em aberto 6 resolvido). Ele é ponto único de
falha da coordenação, não dos dados: com o Juiz fora, os brokers seguem
atendendo enquanto a reserva estiver sincronizada, e o failover fica suspenso
até ele voltar (RF-48).

## Dupla falha: Juiz e reserva fora

Com o Juiz configurado, o primário só confirma escritas sem reserva com o aval
do Juiz (docs/protocolo.md, seção 10). Se o Juiz e a reserva caírem juntos, o
primário não confirma mais nada e se isola: o sistema fica indisponível, em vez
de aceitar leituras que um failover posterior poderia perder (RPO = 0,
RNF-07). Os sensores mantêm as leituras no buffer e o primário volta a atender
quando o Juiz ou a reserva retornam.

## Partição de rede com a conexão de replicação meio-aberta

A reserva recusa a promoção enquanto a conexão de replicação está aberta, o
que evita dois primários quando só a ligação Juiz↔primário cai. Se o primário
morrer sem fechar a conexão (queda de máquina ou de rede, não do processo), a
reserva só percebe pelo keepalive TCP do sistema operacional, o que pode levar
minutos; até lá a promoção fica bloqueada e o RTO de 30 s (RNF-06) não vale.
A queda do processo do primário fecha a conexão e não tem esse atraso.

## Juiz recém-iniciado

O Juiz não guarda estado em disco. Depois de reiniciar, ele espera um prazo de
falha ouvindo os brokers antes de decidir. Se o primário já estiver fora
nesse momento, o Juiz não conhece o último índice dele e não promove a
reserva: o failover exige ter visto o primário.

## Janela mínima ao cancelar uma promoção

Se o primário voltar a falar com o Juiz no mesmo instante em que a reserva
aceita o `PROMOTE`, o Juiz pode manter o antigo primário como ativo; a reserva
recém-promovida recebe `PROMOTE(antigo)` e se isola. Escritas aceitas por ela
nesse intervalo, de milissegundos, não ficam no primário ativo. Como a reserva
só aceita o `PROMOTE` sem conexão de replicação aberta, isso exige o primário
isolado de todos e voltando exatamente durante a promoção.

## Até o antigo primário voltar, a nova primária roda sem reserva

Depois do failover, a reserva promovida atende sozinha enquanto o antigo
primário não volta. Com Juiz e endereço do par configurados, cada escrita
nesse intervalo depende do aval do Juiz, como no primário original. Quando o
antigo primário volta, ele vira reserva e é ressincronizado por completo
(task 014, docs/protocolo.md, seção 10).

## O vip simula o endereço virtual (task 013)

O Etapa 0 e 1 descreve um IP virtual que passa do primário para a reserva.
Um IP virtual real exige root e duas máquinas (ou namespaces de rede) e não
roda nos testes automáticos; por isso ele é simulado pelo vip, um proxy TCP
que leva cada conexão ao broker que aceita o `CONNECT`
(docs/protocolo.md, seção 10).

Consequências:

- **O vip é um processo no caminho dos dados.** Se ele cair, nenhum cliente
  conecta, mesmo com os brokers no ar: é ponto único de falha da simulação.
  Num IP virtual real esse papel é da rede, não de um processo.
- O vip não depende do Juiz: com o Juiz fora, ele continua levando os
  clientes ao primário (RF-48).
- Cada byte passa por um salto a mais, o que soma uma ida e volta local à
  latência.
- Com o primário fora e antes da promoção, o vip recusa os `CONNECT`
  (`vip_rejected_total`); os clientes tentam de novo com backoff.

## Composição do RTO (RNF-06)

O tempo entre a queda do primário e a próxima entrega ao consumidor soma:

1. o prazo de falha do Juiz (padrão 5 s);
2. até uma rodada de avaliação do Juiz (padrão 0,5 s);
3. a espera do backoff de reconexão do cliente, que cresce durante as
   tentativas recusadas (0,1 s dobrando até 30 s por padrão).

Medido com os padrões, matando o primário com `kill -9`: **6,7 s**. No teste
automático, com prazos curtos, cerca de 0,35 s. Um isolamento longo faz o
backoff do cliente crescer; reduzir `ReconnectOptions.MaxDelay` encurta a
volta ao custo de mais tentativas.

## Replicação por par (task 014)

A replicação liga um primário a uma única reserva: cada broker conhece o
próprio endereço de replicação e o do par (DP-12; a escalabilidade horizontal
está fora de escopo, requisitos.md, seção 1.5). A biblioteca cliente, o vip e
o Juiz já lidam com N brokers; só a replicação teria de mudar para N reservas,
trocando o endereço do par por uma lista e esperando a confirmação de todas
(ou de uma maioria) antes do ACK.

## Reinício simultâneo de todos os componentes

O Juiz não guarda estado em disco. Se Juiz e brokers reiniciarem juntos, nenhum
broker anuncia `state = 1` e o Juiz adota o primeiro broker em `state = 4`, que
pode ser o de dados mais antigos; o outro é rebaixado e ressincronizado a
partir dele, perdendo o que só ele tinha. Procedimento: subir primeiro o
broker que era o último primário e esperar o Juiz promovê-lo antes de subir o
outro.

## Broker reiniciado com o Juiz fora

Com Juiz configurado, um broker que sobe como primário só atende depois do
`PROMOTE` (ou da reserva sincronizar nele). Com o Juiz fora e sem reserva
disponível, ele fica sem atender até o Juiz voltar: é o preço de nunca haver
dois primários. Um broker que já estava atendendo continua atendendo.

## Vazão limitada pela gravação síncrona replicada (task 017)

Cada leitura custa, em sequência:
1. fsync no buffer do sensor;
2. fsync no store do primário;
3. ida e volta e fsync na reserva (`REPLICA_ACK`);
4. depois da entrega, um registro de `DELIVER_ACK` que passa pelo mesmo
   caminho.

Todas as gravações do store passam por um único lock, que preserva a ordem
replicada. A vazão total do broker fica, portanto, presa ao tempo de fsync do
disco. Os cenários de `examples/` medem esse teto; os números estão na task 017.
Envio em lote e um fsync por grupo de registros são as alavancas, se o teto
precisar subir.

## A biblioteca cliente limita a "agressividade" de um sensor

A biblioteca envia um `PUBLISH` por vez e espera o `PUBACK` antes do próximo.
Um sensor que a usa, mesmo medindo a cada 1 ms, nunca tem mais de uma leitura
em voo: o excesso fica no buffer dele. O escalonamento round-robin do broker e
o limite de fila por sensor (RF-34, RF-38) protegem contra clientes que **não**
seguem esse ritmo; isso é coberto nos testes do broker
(`internal/broker/backpressure_test.go`). O cenário `justica` mede o efeito de
sensores intensos que usam a biblioteca.

## O consumidor segura até 64 mensagens antes de frear o broker

A biblioteca cliente confirma cada `DELIVER` (`DELIVER_ACK`) assim que a
mensagem entra no canal `Messages()`, que tem 64 posições, e não quando a
aplicação termina de processá-la. Um consumidor lento só faz a fila do broker
crescer depois que esse canal enche. Se a aplicação cair com mensagens no
canal, elas já foram confirmadas e não voltam: a garantia "pelo menos uma vez"
vale até a entrega à aplicação, não até o fim do processamento.

## Um sensor_id, um fluxo de seq (tópicos)

O broker numera e deduplica por `sensor_id`, e o cursor do consumidor também
(`streamKey`); o tópico não entra na chave. Consequências:

- **Vários sensores no mesmo tópico funciona**: cada um tem o próprio
  `sensor_id`, `seq` e buffer (`-topico` sem `{id}`).
- **Um mesmo `sensor_id` em dois tópicos compartilha um único fluxo de
  `seq`.** Dois clientes com o mesmo id e buffers independentes começam ambos
  em 1, e o broker trata as leituras do segundo como repetidas (confirma e
  não entrega).
- O buffer durável do sensor guarda `seq`, instante e temperatura, **sem
  tópico**: ele atende um tópico por cliente (`SensorOptions.Topic`).

Para publicar em outro tópico, o `sensor-manual publicar` usa um `sensor_id`
de fluxo próprio (`<id>.<último nível>`), com `seq` iniciado no relógio, e
**sem buffer durável**. A alternativa de ter vários tópicos no mesmo
`sensor_id` exigiria chavear `seq`, cursor e store por (`sensor_id`, tópico)
ou gravar o tópico em cada leitura do buffer; ambas mexem em persistência e
replicação e não foram feitas.
