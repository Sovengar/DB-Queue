package jon.db.queue.store;

import jon.db.queue.api.QueueSpringJpaRepo;
import jon.db.queue.models.AlertQueue;

public interface AlertQueueSpringJpaRepo extends QueueSpringJpaRepo<AlertQueue, Long> {
}

