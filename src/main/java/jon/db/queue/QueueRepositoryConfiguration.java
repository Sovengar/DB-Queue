package jon.db.queue;

import jakarta.persistence.EntityManager;
import jon.db.queue.api.QueuePostgreRepo;
import jon.db.queue.api.QueueRepo;
import jon.db.queue.models.AlertQueue;
import jon.db.queue.models.GenericQueue;
import jon.db.queue.store.AlertQueueSpringJpaRepo;
import jon.db.queue.store.GenericQueueSpringJpaRepo;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class QueueRepositoryConfiguration {
    @Bean
    public QueueRepo<GenericQueue, Long> genericQueueRepository(EntityManager entityManager, GenericQueueSpringJpaRepo genericQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, genericQueueJpaRepo, GenericQueue.class);
    }
    
    @Bean
    public QueueRepo<AlertQueue, Long> alertQueueRepository(EntityManager entityManager, AlertQueueSpringJpaRepo alertQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, alertQueueJpaRepo, AlertQueue.class);
    }
}
