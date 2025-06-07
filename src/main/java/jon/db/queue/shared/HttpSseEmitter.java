package jon.db.queue.shared;

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
public class HttpSseEmitter {
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

    public void sendMessageCreated(final String id, final Map<String, Object> data) {
        List<String> deadEmitters = new CopyOnWriteArrayList<>();

        emitters.forEach((client, emitter) -> {
            try {
                var sseEvent = SseEmitter.event().id(id).name("message").data(data);
                emitter.send(sseEvent);
                log.trace("Message sent to client: {}", client);
            } catch (IOException e) {
                log.warn("Failed to send message to client: {}", client, e);
                deadEmitters.add(client);
            }
        });

        // Clean up dead emitters
        deadEmitters.forEach(emitters::remove);
    }

    public void sendMessageUpdated(final String id, final Map<String, Object> data) {
        List<String> deadEmitters = new CopyOnWriteArrayList<>();

        emitters.forEach((client, emitter) -> {
            try {
                var sseEvent = SseEmitter.event().id(id).name("update").data(data);
                emitter.send(sseEvent);
                log.trace("Message sent to client: {}", client);
            } catch (IOException e) {
                log.warn("Failed to send message to client: {}", client, e);
                deadEmitters.add(client);
            }
        });

        // Clean up dead emitters
        deadEmitters.forEach(emitters::remove);
    }

    public void sendMessageDeleted(final String id, final Map<String, Object> data) {
        List<String> deadEmitters = new CopyOnWriteArrayList<>();

        emitters.forEach((client, emitter) -> {
            try {
                var sseEvent = SseEmitter.event().id(id).name("delete").data(data);
                emitter.send(sseEvent);
                log.trace("Message sent to client: {}", client);
            } catch (IOException e) {
                log.warn("Failed to send message to client: {}", client, e);
                deadEmitters.add(client);
            }
        });

        // Clean up dead emitters
        deadEmitters.forEach(emitters::remove);
    }

    public int getActiveConnectionsCount() {
        return emitters.size();
    }
}
