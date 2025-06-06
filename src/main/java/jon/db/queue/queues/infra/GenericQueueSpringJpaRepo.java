package jon.db.queue.queues.infra;

import jon.db.queue.queues.api.queue.QueueSpringJpaRepo;
import jon.db.queue.queues.models.GenericQueue;

import java.util.List;

public interface GenericQueueSpringJpaRepo extends QueueSpringJpaRepo<GenericQueue, Long> {
    List<GenericQueue> findTop20ByOrderByInternalIdDesc();
}
