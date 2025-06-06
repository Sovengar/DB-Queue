package jon.db.queue.api.dead_letter_queue.application;

import jon.db.queue.api.dead_letter_queue.DeadLetterQueueHandler;
import jon.db.queue.api.dead_letter_queue.DLQRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/dead-letter-queue")
@RequiredArgsConstructor
class MarkAsProcessed {
    private final DLQRepo repo;
    private final DeadLetterQueueHandler service;

    @PutMapping("/processed/{messageId}")
    public ResponseEntity<Void> discardMessage(final @PathVariable UUID messageId) {
        Assert.notNull(messageId, "Message ID cannot be null");

        service.markAsProcessed(messageId);
        return ResponseEntity.ok().build();
    }
}
