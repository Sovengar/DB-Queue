package jon.db.queue.store;

import jon.db.queue.models.GenericQueue;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface GenericQueueSpringJpaRepo extends JpaRepository<GenericQueue, Long> {
    GenericQueue findByMessageId(UUID messageId);

    List<GenericQueue> findTop20ByOrderByInternalIdDesc();
}
