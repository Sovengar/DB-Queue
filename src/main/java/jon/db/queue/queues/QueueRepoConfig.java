package jon.db.queue.queues;

import jakarta.persistence.EntityManager;
import jon.db.queue.characters.character_queue.infra.CharacterQueueSpringJpaRepo;
import jon.db.queue.queues.product_queue.ProductQueueSpringJpaRepo;
import jon.db.queue.shared.queue.abstract_queue.QueuePostgreRepo;
import jon.db.queue.shared.queue.abstract_queue.QueueRepo;
import jon.db.queue.queues.product_queue.ProductQueue;
import jon.db.queue.characters.character_queue.CharacterQueue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class QueueRepoConfig {
    @Bean
    public QueueRepo<CharacterQueue, Long> characterQueueRepository(EntityManager entityManager, CharacterQueueSpringJpaRepo characterQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, characterQueueJpaRepo, CharacterQueue.class);
    }
    
    @Bean
    public QueueRepo<ProductQueue, Long> productQueueRepository(EntityManager entityManager, ProductQueueSpringJpaRepo productQueueJpaRepo) {
        return new QueuePostgreRepo<>(entityManager, productQueueJpaRepo, ProductQueue.class);
    }
}
