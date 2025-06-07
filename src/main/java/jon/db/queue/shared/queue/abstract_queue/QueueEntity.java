package jon.db.queue.shared.queue.abstract_queue;

import java.time.LocalDateTime;
import java.util.UUID;

public interface QueueEntity<ID> {
    ID getInternalId();
    UUID getMessageId();
    LocalDateTime getArrivedAt();
    LocalDateTime getProcessedAt();
    Integer getNonTimeoutRetries();
}
