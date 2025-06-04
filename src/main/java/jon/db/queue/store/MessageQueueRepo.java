package jon.db.queue.store;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Table;
import jon.db.queue.models.QueueMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface MessageQueueRepo {
    QueueMessage findByMessageId(UUID messageId);

    List<QueueMessage> lockNextMessages(int batchSize, int maxRetries);

    List<QueueMessage> lockPoisonedMessages();

    long countLockedRows();

    Long create(QueueMessage queueMessage);

    void update(QueueMessage queueMessage);

    void delete(QueueMessage queueMessage);
}

@Repository
@Transactional
@RequiredArgsConstructor
@Slf4j
class MessageMessageQueuePosgreSQLRepo implements MessageQueueRepo {
    @PersistenceContext
    private final EntityManager entityManager;
    private final MessageQueueSpringJpaRepo messageQueueSpringJpaRepo;

    @Override
    public QueueMessage findByMessageId(final UUID messageId) {
        return messageQueueSpringJpaRepo.findByMessageId(messageId);
    }

    @Override
    public List<QueueMessage> lockNextMessages(int batchSize, int maxRetries) {
        return entityManager
                .createNativeQuery("""
                SELECT * FROM queue_message 
                WHERE processed_at IS NULL
                AND non_timeout_retries <= :maxRetries   
                ORDER BY internal_id ASC
                FETCH FIRST :batchSize ROWS ONLY
                FOR NO KEY UPDATE SKIP LOCKED
                """, QueueMessage.class)
                .setParameter("batchSize", batchSize)
                .setParameter("maxRetries", maxRetries)
                .getResultList();
    }

    @Override
    public List<QueueMessage> lockPoisonedMessages() {
        return entityManager
                .createNativeQuery("""
            SELECT * FROM queue_message 
            WHERE processed_at IS NULL
            AND arrived_at < :oneHourAgo
            ORDER BY internal_id ASC
            FOR NO KEY UPDATE SKIP LOCKED
            """, QueueMessage.class)
                .setParameter("oneHourAgo", LocalDateTime.now().minusHours(1))
                .getResultList();
    }

    @Override
    public long countLockedRows() {
        String tableName = QueueMessage.class.getAnnotation(Table.class).name();

        String sql = """
            SELECT count(*)
            FROM pg_locks l
            JOIN pg_class t ON l.relation = t.oid
            WHERE t.relname = :tableName
              AND l.mode = 'RowExclusiveLock'
              AND l.granted
        """;

        log.info(sql);

        Object result = entityManager.createNativeQuery(sql)
                .setParameter("tableName", tableName)
                .getSingleResult();

        return ((Number) result).longValue();
    }

    @Override
    public Long create(final QueueMessage queueMessage) {
        var msg = messageQueueSpringJpaRepo.save(queueMessage);
        return msg.getInternalId();
    }

    @Override
    public void update(QueueMessage msg) {
        messageQueueSpringJpaRepo.save(msg);
    }

    @Override
    public void delete(final QueueMessage queueMessage) {
        messageQueueSpringJpaRepo.delete(queueMessage);
    }
}
