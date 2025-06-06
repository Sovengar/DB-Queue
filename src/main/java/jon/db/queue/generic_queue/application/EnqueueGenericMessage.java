package jon.db.queue.generic_queue.application;

import jon.db.queue.api.queue.QueueRepo;
import jon.db.queue.generic_queue.GenericQueueProducer;
import jon.db.queue.generic_queue.GenericQueueSseEmitter;
import jon.db.queue.models.GenericQueue;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/generic-queue")
@RequiredArgsConstructor
class EnqueueGenericMessage {
    private final QueueRepo<GenericQueue, Long> repo;

    private final GenericQueueProducer producer;
    private final GenericQueueSseEmitter genericQueueSseEmitter;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody EnqueueRequest request) {
        producer.publish(request.messageId(), request.data());
        var message = repo.findByMessageId(request.messageId()).orElseThrow();
        genericQueueSseEmitter.sendMessageToAllClients(message);

        return ResponseEntity.ok().build();
    }

    record EnqueueRequest(UUID messageId, Map<String, Object> data) { }
}
