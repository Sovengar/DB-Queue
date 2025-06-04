package jon.db.queue;

import jon.db.queue.models.DeadLetterQueue;
import jon.db.queue.store.MessageQueueRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
class DeadLetterQueueService {
    private final DLQRepo repo;

    public void create(DeadLetterQueue deadLetterQueue) {
        repo.create(deadLetterQueue);
    }

    public void discard(UUID messageId) {
        repo.delete(messageId);
    }
}

interface DLQRepo {
    List<DeadLetterQueue> findAll(); //Could throw an OutOfMemory if it comes to a massive influx of failed messages
    void create(DeadLetterQueue deadLetterQueue);
    void delete(UUID messageId);
}

interface DLQSpringJPARepo extends JpaRepository <DeadLetterQueue, UUID> { }

@Repository
@RequiredArgsConstructor
class DLQPosgreSQLRepo implements DLQRepo {
    private final DLQSpringJPARepo DLQSpringJPARepo;

    @Override
    public List<DeadLetterQueue> findAll() {
        return DLQSpringJPARepo.findAll();
    }

    @Override
    public void create(DeadLetterQueue deadLetterQueue) {
        DLQSpringJPARepo.save(deadLetterQueue);
    }

    @Override
    public void delete(UUID messageId) {
        DLQSpringJPARepo.deleteById(messageId);
    }
}
