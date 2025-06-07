package jon.db.queue.queues;

import jakarta.persistence.EntityManager;
import jon.db.queue.queues.generic_queue.infra.GenericQueueSpringJpaRepo;
import jon.db.queue.queues.product_queue.ProductQueueSpringJpaRepo;
import jon.db.queue.shared.queue.abstract_queue.QueuePostgreRepo;
import jon.db.queue.shared.queue.abstract_queue.QueueRepo;
import jon.db.queue.queues.product_queue.ProductQueue;
import jon.db.queue.queues.generic_queue.GenericQueue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class QueueRepoConfig {
    @Bean
    public QueueRepo<GenericQueue, Long> genericQueueRepository(EntityManager entityManager, GenericQueueSpringJpaRepo genericQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, genericQueueJpaRepo, GenericQueue.class);
    }
    
    @Bean
    public QueueRepo<ProductQueue, Long> productQueueRepository(EntityManager entityManager, ProductQueueSpringJpaRepo productQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, productQueueJpaRepo, ProductQueue.class);
    }
}
