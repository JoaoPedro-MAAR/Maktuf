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
