package jon.db.queue;

import jakarta.persistence.EntityManager;
import jon.db.queue.models.DeadLetterQueue;
import jon.db.queue.models.QueueMessage;
import jon.db.queue.store.MessageQueueRepo;
import jon.db.queue.store.MessageQueueSpringJpaRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/msg-queue")
@RequiredArgsConstructor
class MessageQueueHttpController {
    private final MessageQueueRepo repo;
    private final MessageQueueCreator mqCreator;
    private final SseEmitterService sseEmitterService;
    private final MessageQueueSpringJpaRepo jpaRepo;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody EnqueueRequest request) {
        mqCreator.createProvidingData(request.data(), request.messageId());
        var message = repo.findByMessageId(request.messageId());
        sseEmitterService.sendMessageToAllClients(message);

        return ResponseEntity.ok().build();
    }

    @GetMapping("/locked-count")
    public String getLockedCount() {
        var lockedRows = repo.countLockedRows();
        return String.format("There are %s rows locked", lockedRows);
    }

    /**
     * SSE Endpoint to retrieve messages in real time
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamMessages() {
        SseEmitter emitter = sseEmitterService.createEmitter();

        try {
            var messages = findLast20Msgs();

            for (QueueMessage message : messages) {
                var data = sseEmitterService.buildDataFromEntity(message);
                var sseEvent = SseEmitter.event().id(String.valueOf(message.getInternalId())).name("message").data(data);
                emitter.send(sseEvent);
            }
        } catch (IOException e) {
            emitter.completeWithError(e);
        }

        return emitter;
    }

    @GetMapping("/active-connections")
    public ResponseEntity<Map<String, Integer>> getActiveConnections() {
        return ResponseEntity.ok(Map.of("connections", sseEmitterService.getActiveConnectionsCount()));
    }

    public List<QueueMessage> findLast20Msgs() {
        return jpaRepo.findTop20ByOrderByInternalIdDesc();
    }

    record EnqueueRequest(UUID messageId, Map<String, Object> data) { }
}

@RestController
@RequestMapping("/dead-letter-queue")
@RequiredArgsConstructor
class DeadLetterQueueHttpController {
    private final DLQRepo repo;
    private final DeadLetterQueueService service;

    @GetMapping("/search")
    public ResponseEntity<SearchResponse> getAllMessages() {
        var messages = repo.findAll();
        var messageIds = messages.stream().map(DeadLetterQueue::getMessageId).toList();
        return ResponseEntity.ok(new SearchResponse(messageIds, "1.0.0", LocalDateTime.now().toString()));
    }

    @DeleteMapping("/discard/{messageId}")
    public ResponseEntity<Void> discardMessage(final @PathVariable UUID messageId) {
        Assert.notNull(messageId, "Message ID cannot be null");

        service.discard(messageId);
        return ResponseEntity.ok().build();
    }

    record SearchResponse(List<UUID> messageId, String version, String retrievedAt) {
    }
}
