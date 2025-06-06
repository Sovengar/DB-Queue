package jon.db.queue.queues.infra;

import jakarta.persistence.EntityManager;
import jon.db.queue.queues.api.queue.QueuePostgreRepo;
import jon.db.queue.queues.api.queue.QueueRepo;
import jon.db.queue.queues.models.AlertQueue;
import jon.db.queue.queues.models.GenericQueue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class QueueRepoConfig {
    @Bean
    public QueueRepo<GenericQueue, Long> genericQueueRepository(EntityManager entityManager, GenericQueueSpringJpaRepo genericQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, genericQueueJpaRepo, GenericQueue.class);
    }
    
    @Bean
    public QueueRepo<AlertQueue, Long> alertQueueRepository(EntityManager entityManager, AlertQueueSpringJpaRepo alertQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, alertQueueJpaRepo, AlertQueue.class);
    }
}
