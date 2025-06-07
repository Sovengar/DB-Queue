package jon.db.queue.characters.character_queue.application;

import jon.db.queue.shared.HttpSseEmitter;
import jon.db.queue.characters.character_queue.CharacterQueue;
import jon.db.queue.characters.character_queue.infra.CharacterQueueSpringJpaRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/generic-queue")
@RequiredArgsConstructor
class StreamLast20Msgs {
    private final HttpSseEmitter httpSseEmitter;
    private final CharacterQueueSpringJpaRepo jpaRepo;

    /**
     * SSE Endpoint to retrieve messages in real time
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamMessages() {
        SseEmitter emitter = httpSseEmitter.createEmitter();

        try {
            var messages = findLast20Msgs();

            for (CharacterQueue message : messages) {
                var data = message.transformFieldsToMap();
                var sseEvent = SseEmitter.event().id(String.valueOf(message.getInternalId())).name("message").data(data);
                emitter.send(sseEvent);
            }
        } catch (IOException e) {
            emitter.completeWithError(e);
        }

        return emitter;
    }

    @GetMapping("/stream/active-connections")
    public ResponseEntity<Map<String, Integer>> getActiveConnections() {
        return ResponseEntity.ok(Map.of("connections", httpSseEmitter.getActiveConnectionsCount()));
    }

    public List<CharacterQueue> findLast20Msgs() {
        return jpaRepo.findTop20ByOrderByInternalIdDesc();
    }
}

