package jon.db.queue.generic_queue.application;

import jon.db.queue.generic_queue.GenericQueueSseEmitter;
import jon.db.queue.models.QueueMessage;
import jon.db.queue.store.MessageQueueSpringJpaRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/generic-queue")
@RequiredArgsConstructor
class StreamLast20Msgs {
    private final GenericQueueSseEmitter genericQueueSseEmitter;
    private final MessageQueueSpringJpaRepo jpaRepo;

    /**
     * SSE Endpoint to retrieve messages in real time
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamMessages() {
        SseEmitter emitter = genericQueueSseEmitter.createEmitter();

        try {
            var messages = findLast20Msgs();

            for (QueueMessage message : messages) {
                var data = genericQueueSseEmitter.buildDataFromEntity(message);
                var sseEvent = SseEmitter.event().id(String.valueOf(message.getInternalId())).name("message").data(data);
                emitter.send(sseEvent);
            }
        } catch (IOException e) {
            emitter.completeWithError(e);
        }

        return emitter;
    }

    public List<QueueMessage> findLast20Msgs() {
        return jpaRepo.findTop20ByOrderByInternalIdDesc();
    }
}

