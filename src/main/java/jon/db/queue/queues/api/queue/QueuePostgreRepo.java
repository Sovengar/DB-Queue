package jon.db.queue.queues.api.queue;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jon.db.queue.queues.api.MessageDuplicatedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Transactional
@RequiredArgsConstructor
@Slf4j
public class QueuePostgreRepo<T extends QueueEntity<ID>, ID> implements QueueRepo<T, ID> {
    @PersistenceContext
    private final EntityManager entityManager;
    private final QueueSpringJpaRepo<T, ID> queueRepo;

    private final Class<T> entityClass;

    @Override
    public Optional<T> findById(final ID id) {
        return queueRepo.findById(id);
    }

    @Override
    public Optional<T> findByMessageId(final UUID messageId) {
        return queueRepo.findByMessageId(messageId);
    }

    @Override
    public List<T> lockNextMessages(String tableName, int batchSize, int maxRetries) {
        validateTableNameForSQLInjection(tableName);

        var sql = "SELECT * FROM " + tableName +
                " WHERE processed_at IS NULL " +
                "AND non_timeout_retries <= :maxRetries " +
                "ORDER BY internal_id ASC " +
                "FETCH FIRST :batchSize ROWS ONLY " +
                "FOR NO KEY UPDATE SKIP LOCKED";

        return entityManager
                .createNativeQuery(sql, entityClass)
                .setParameter("batchSize", batchSize)
                .setParameter("maxRetries", maxRetries)
                .getResultList();
    }

    @Override
    public List<T> lockPoisonedMessages(String tableName) {
        validateTableNameForSQLInjection(tableName);

        var sql = "SELECT * FROM " + tableName +
                " WHERE processed_at IS NULL " +
                " AND arrived_at < :oneHourAgo " +
                " ORDER BY internal_id ASC " +
                " FOR NO KEY UPDATE SKIP LOCKED ";

        return entityManager
                .createNativeQuery(sql, entityClass)
                .setParameter("oneHourAgo", LocalDateTime.now().minusHours(1))
                .getResultList();
    }

    private void validateTableNameForSQLInjection(String tableName) {
        if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
            throw new IllegalArgumentException("Nombre de tabla inválido: " + tableName);
        }
    }

    @Override
    public long countLockedRows(String tableName) {
        validateTableNameForSQLInjection(tableName);

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
    public ID create(final T entity) {
        try {
            T savedEntity = queueRepo.save(entity);
            return savedEntity.getInternalId();
        } catch (DataIntegrityViolationException e) {
                throw new MessageDuplicatedException(entity.getMessageId());
        }
    }

    @Override
    public void update(T entity) {
        queueRepo.save(entity);
    }

    @Override
    public void delete(final T entity) {
        queueRepo.delete(entity);
    }
}
