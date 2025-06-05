package jon.db.queue.generic_queue.application;

import jon.db.queue.generic_queue.GenericQueueCreator;
import jon.db.queue.generic_queue.GenericQueueSseEmitter;
import jon.db.queue.store.MessageQueueRepo;
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
    private final MessageQueueRepo repo;
    private final GenericQueueCreator mqCreator;
    private final GenericQueueSseEmitter genericQueueSseEmitter;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody EnqueueRequest request) {
        mqCreator.createProvidingData(request.data(), request.messageId());
        var message = repo.findByMessageId(request.messageId());
        genericQueueSseEmitter.sendMessageToAllClients(message);

        return ResponseEntity.ok().build();
    }

    record EnqueueRequest(UUID messageId, Map<String, Object> data) { }
}
