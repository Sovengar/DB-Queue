package jon.db.queue.api.queue;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.NoRepositoryBean;

import java.util.Optional;
import java.util.UUID;

@NoRepositoryBean
public interface QueueSpringJpaRepo<T extends QueueEntity<ID>, ID> extends JpaRepository<T, ID> {
    Optional<T> findByMessageId(UUID messageId);
}

