package jon.db.queue.queues.generic_queue.infra;

import jon.db.queue.shared.queue.abstract_queue.QueueSpringJpaRepo;
import jon.db.queue.queues.generic_queue.GenericQueue;

import java.util.List;

public interface GenericQueueSpringJpaRepo extends QueueSpringJpaRepo<GenericQueue, Long> {
    List<GenericQueue> findTop20ByOrderByInternalIdDesc();
}
