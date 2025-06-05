package jon.db.queue.generic_queue;

import jon.db.queue.models.QueueMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class GenericQueueSseEmitter {
    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();
    private final AtomicLong emitterCounter = new AtomicLong(0);

    public SseEmitter createEmitter() {
        String emitterId = String.valueOf(emitterCounter.incrementAndGet());
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE); // No timeout

        // Callback when completed
        emitter.onCompletion(() -> {
            log.info("SSE connection completed for client: {}", emitterId);
            emitters.remove(emitterId);
        });

        // Callback when timed out
        emitter.onTimeout(() -> {
            log.info("SSE connection timed out for client: {}", emitterId);
            emitter.complete();
            emitters.remove(emitterId);
        });

        // Callback when error occurred
        emitter.onError(ex -> {
            log.error("SSE error for client: {}", emitterId, ex);
            emitters.remove(emitterId);
        });

        emitters.put(emitterId, emitter);
        log.info("New SSE connection established with client: {}", emitterId);

        return emitter;
    }

    public void sendMessageToAllClients(QueueMessage message) {
        List<String> deadEmitters = new CopyOnWriteArrayList<>();

        emitters.forEach((id, emitter) -> {
            try {
                var sseEvent = SseEmitter.event()
                        .id(String.valueOf(message.getInternalId()))
                        .name("message")
                        .data(buildDataFromEntity(message));

                emitter.send(sseEvent);
                log.debug("Message sent to client: {}", id);
            } catch (IOException e) {
                log.warn("Failed to send message to client: {}", id, e);
                deadEmitters.add(id);
            }
        });

        // Clean up dead emitters
        deadEmitters.forEach(emitters::remove);
    }

    public void sendMessageUpdate(QueueMessage msg) {
        var sseEvent = SseEmitter.event()
                .id(String.valueOf(msg.getInternalId()))
                .name("update") //Specific event for updates
                .data(buildDataFromEntity(msg));

        emitters.forEach((eventId, emitter) -> {
            try {
                emitter.send(sseEvent);
            } catch (IOException e) {
                //TODO
            }
        });
    }

    public void sendMessageDelete(QueueMessage msg) {
        var sseEvent = SseEmitter.event()
                .id(String.valueOf(msg.getInternalId()))
                .name("delete") //Specific event for deletes
                .data(buildDataFromEntity(msg));

        emitters.forEach((eventId, emitter) -> {
            try {
                emitter.send(sseEvent);
            } catch (IOException e) {
                //TODO
            }
        });
    }

    public Map<String, Object> buildDataFromEntity(QueueMessage message){
        return Map.of(
                "internalId", message.getInternalId(),
                "messageId", message.getMessageId(),
                "data", message.getData(),
                "arrivedAt", message.getArrivedAt(),
                "nonTimeoutRetries", message.getNonTimeoutRetries(),
                "processedAt", message.getProcessedAt() != null ? message.getProcessedAt().toString() : ""
        );
    }

    public void sendEventsToAllClients(List<QueueMessage> messages) {
        messages.forEach(this::sendMessageToAllClients);
    }

    public int getActiveConnectionsCount() {
        return emitters.size();
    }
}
