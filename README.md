Inicia el proceso con Docker encendido y pulsando play en Application.java

Accede a http://localhost:8080/character-queue-monitor.html para ver como se van procesando los mensajes

Puedes añadir mensajes manualmente y/o removerlos de la DLQ.

Ten una queue por caso de uso, aqui se llama generica pero porque no es especifica, no es que sea realmente generica.

Se cargan mensajes cada pocos segundos de forma automatica.
De la misma forma, se va ejecutando el proceso que hace polling en la BD para procesarlos.
Cuando se procesa, hay una chance de que de throw, a proposito para incrementar el contador de error.

Si excede los 3 retries, se mueve a la DLQ.
Si lleva mas de 1 hora sin haber sido procesado (processedAt NULL), se mueve a la DLQ.


------------------------

[Worker-2] Processing 3 messages [842, 851, 852]
[Worker-2] Error processing message 842: Simulating Random error
[Worker-2] Message 842 with id 4d613215-e0c4-4e19-b2a8-c6b346821715 has reached the maximum number of retries (3), moving to Dead Letter Queue

[Worker-1] Processing 3 messages [853, 860, 861]
[Worker-1] Error processing message 853: Simulating Random error
[Worker-1] Error processing message 861: Simulating Random error
[Worker-2] Processing 3 messages [853, 861, 862]

Diria que es porque al haber error, la transaccion no sigue y por tanto se liberan los 3
