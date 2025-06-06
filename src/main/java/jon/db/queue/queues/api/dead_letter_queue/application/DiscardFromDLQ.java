package jon.db.queue.queues.api.dead_letter_queue.application;

import jon.db.queue.queues.api.dead_letter_queue.DeadLetterQueueHandler;
import jon.db.queue.queues.api.dead_letter_queue.DLQRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/dead-letter-queue")
@RequiredArgsConstructor
class DiscardFromDLQ {
    private final DLQRepo repo;
    private final DeadLetterQueueHandler service;

    @DeleteMapping("/discard/{messageId}")
    public ResponseEntity<Void> discardMessage(final @PathVariable UUID messageId) {
        Assert.notNull(messageId, "Message ID cannot be null");

        service.discard(messageId);
        return ResponseEntity.ok().build();
    }
}
