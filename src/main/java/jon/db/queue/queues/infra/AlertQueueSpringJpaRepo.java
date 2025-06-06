package jon.db.queue.queues.infra;

import jon.db.queue.queues.api.queue.QueueSpringJpaRepo;
import jon.db.queue.queues.models.AlertQueue;

public interface AlertQueueSpringJpaRepo extends QueueSpringJpaRepo<AlertQueue, Long> {
}

