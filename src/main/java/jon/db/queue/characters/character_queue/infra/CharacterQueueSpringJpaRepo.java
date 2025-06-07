package jon.db.queue.characters.character_queue.infra;

import jon.db.queue.characters.character_queue.CharacterQueue;
import jon.db.queue.shared.queue.abstract_queue.QueueSpringJpaRepo;

import java.util.List;

public interface CharacterQueueSpringJpaRepo extends QueueSpringJpaRepo<CharacterQueue, Long> {
    List<CharacterQueue> findTop20ByOrderByInternalIdDesc();
}
