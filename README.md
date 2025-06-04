Inicia el proceso con Docker encendido y pulsando play en Application.java

Accede a http://localhost:8080/message-queue-monitor.html para ver como se van procesando los mensajes

Puedes añadir mensajes manualmente y/o removerlos de la DLQ.


Se cargan mensajes cada pocos segundos de forma automatica.
De la misma forma, se va ejecutando el proceso que hace polling en la BD para procesarlos.
Cuando se procesa, hay una chance de que de throw, a proposito para incrementar el contador de error.
Si excede los 3 retries, se mueve a la DLQ.
Si lleva mas de 1 hora sin haber sido procesado (processedAt NULL), se mueve a la DLQ.

