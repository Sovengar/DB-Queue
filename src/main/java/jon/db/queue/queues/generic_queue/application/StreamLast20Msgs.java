package jon.db.queue.queues.generic_queue.application;

import jon.db.queue.queues.infra.HttpSseEmitter;
import jon.db.queue.queues.models.GenericQueue;
import jon.db.queue.queues.infra.GenericQueueSpringJpaRepo;
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
    private final HttpSseEmitter httpSseEmitter;
    private final GenericQueueSpringJpaRepo jpaRepo;

    /**
     * SSE Endpoint to retrieve messages in real time
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamMessages() {
        SseEmitter emitter = httpSseEmitter.createEmitter();

        try {
            var messages = findLast20Msgs();

            for (GenericQueue message : messages) {
                var data = message.transformFieldsToMap();
                var sseEvent = SseEmitter.event().id(String.valueOf(message.getInternalId())).name("message").data(data);
                emitter.send(sseEvent);
            }
        } catch (IOException e) {
            emitter.completeWithError(e);
        }

        return emitter;
    }

    public List<GenericQueue> findLast20Msgs() {
        return jpaRepo.findTop20ByOrderByInternalIdDesc();
    }
}

