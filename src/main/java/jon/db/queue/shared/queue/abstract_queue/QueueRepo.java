package jon.db.queue.shared.queue.abstract_queue;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface QueueRepo<T extends QueueEntity<ID>, ID> {
    Optional<T> findById(ID id);
    Optional<T> findByMessageId(UUID messageId);

    List<T> lockNextMessages(String tableName, int batchSize, int maxRetries);

    List<T> lockPoisonedMessages(String tableName);

    int deleteOldMessages(String tableName);

    long countLockedRows(String tableName);

    ID create(T entity);

    void update(T entity);

    void delete(T entity);
}


