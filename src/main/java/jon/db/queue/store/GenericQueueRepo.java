package jon.db.queue.store;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Table;
import jon.db.queue.models.GenericQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface GenericQueueRepo {
    GenericQueue findByMessageId(UUID messageId);

    List<GenericQueue> lockNextMessages(String tableName, int batchSize, int maxRetries);

    List<GenericQueue> lockPoisonedMessages(String tableName);

    long countLockedRows(String tableName);

    Long create(GenericQueue genericQueue);

    void update(GenericQueue genericQueue);

    void delete(GenericQueue genericQueue);
}

@Repository
@Transactional
@RequiredArgsConstructor
@Slf4j
class GenericQueuePosgreSQLRepo implements GenericQueueRepo {
    @PersistenceContext
    private final EntityManager entityManager;
    private final GenericQueueSpringJpaRepo genericQueueSpringJpaRepo;

    @Override
    public GenericQueue findByMessageId(final UUID messageId) {
        return genericQueueSpringJpaRepo.findByMessageId(messageId);
    }

    @Override
    public List<GenericQueue> lockNextMessages(String tableName, int batchSize, int maxRetries) {
        var sql = "SELECT * FROM " + tableName +
                " WHERE processed_at IS NULL " +
                "AND non_timeout_retries <= :maxRetries " +
                "ORDER BY internal_id ASC " +
                "FETCH FIRST :batchSize ROWS ONLY " +
                "FOR NO KEY UPDATE SKIP LOCKED";

        return entityManager
                .createNativeQuery(sql, GenericQueue.class)
                .setParameter("batchSize", batchSize)
                .setParameter("maxRetries", maxRetries)
                .getResultList();
    }

    @Override
    public List<GenericQueue> lockPoisonedMessages(String tableName) {
        var sql = "SELECT * FROM " + tableName +
            " WHERE processed_at IS NULL " +
            " AND arrived_at < :oneHourAgo " +
            " ORDER BY internal_id ASC " +
            " FOR NO KEY UPDATE SKIP LOCKED ";

        return entityManager
                .createNativeQuery(sql, GenericQueue.class)
                .setParameter("oneHourAgo", LocalDateTime.now().minusHours(1))
                .getResultList();
    }

    @Override
    public long countLockedRows(String tableName) {
        String sql = """
            SELECT count(*)
            FROM pg_locks l
            JOIN pg_class t ON l.relation = t.oid
            WHERE t.relname = :tableName
              AND l.mode = 'RowExclusiveLock'
              AND l.granted
        """;

        Object result = entityManager.createNativeQuery(sql)
                .setParameter("tableName", tableName)
                .getSingleResult();

        return ((Number) result).longValue();
    }

    @Override
    public Long create(final GenericQueue genericQueue) {
        var msg = genericQueueSpringJpaRepo.save(genericQueue);
        return msg.getInternalId();
    }

    @Override
    public void update(GenericQueue msg) {
        genericQueueSpringJpaRepo.save(msg);
    }

    @Override
    public void delete(final GenericQueue genericQueue) {
        genericQueueSpringJpaRepo.delete(genericQueue);
    }
}
