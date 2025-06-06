package jon.db.queue.dead_letter_queue;

import jon.db.queue.api.DeadLetterQueue;
import jon.db.queue.api.DLQRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class DeadLetterQueueHandler {
    private final DLQRepo repo;

    public void create(DeadLetterQueue deadLetterQueue) {
        repo.create(deadLetterQueue);
    }

    public void discard(UUID messageId) {
        repo.delete(messageId);
    }

    public void markAsProcessed(UUID messageId) {
        repo.delete(messageId);
    }
}
