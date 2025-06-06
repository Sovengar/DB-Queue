package jon.db.queue.queues.generic_queue.application;

import jon.db.queue.queues.generic_queue.infra.GenericQueueProducer;
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
    private final GenericQueueProducer producer;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody EnqueueRequest request) {
        producer.publish(request.messageId(), request.data());

        return ResponseEntity.ok().build();
    }

    record EnqueueRequest(UUID messageId, Map<String, Object> data) { }
}
