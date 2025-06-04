package jon.db.queue.store;

import jakarta.persistence.LockModeType;
import jon.db.queue.models.QueueMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface MessageQueueSpringJpaRepo extends JpaRepository<QueueMessage, Long> {
    //TODO TEST
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query(value = """
            SELECT * FROM queue_message  
            WHERE status = 'PENDING' 
            ORDER BY id 
            LIMIT 5 
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<QueueMessage> findNextMessages();

    QueueMessage findByMessageId(UUID messageId);

    List<QueueMessage> findTop20ByOrderByInternalIdDesc();
}
