package jon.db.queue.store;

import jon.db.queue.api.queue.QueueSpringJpaRepo;
import jon.db.queue.models.GenericQueue;

import java.util.List;

public interface GenericQueueSpringJpaRepo extends QueueSpringJpaRepo<GenericQueue, Long> {
    List<GenericQueue> findTop20ByOrderByInternalIdDesc();
}
