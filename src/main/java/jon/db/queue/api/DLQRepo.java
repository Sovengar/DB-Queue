package jon.db.queue.api;

import lombok.RequiredArgsConstructor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

public interface DLQRepo {
    List<DeadLetterQueue> findAll(); //Could throw an OutOfMemory if it comes to a massive influx of failed messages

    void create(DeadLetterQueue deadLetterQueue);

    void delete(UUID messageId);
}

@Repository
@RequiredArgsConstructor
class DLQPosgreRepo implements DLQRepo {
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

interface DLQSpringJPARepo extends JpaRepository<DeadLetterQueue, UUID> { }
