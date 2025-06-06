package jon.db.queue.queues.generic_queue.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jon.db.queue.queues.Emitter;
import jon.db.queue.queues.api.queue.QueueRepo;
import jon.db.queue.queues.models.GenericQueue;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class GenericMessageCreator {
    private final QueueRepo<GenericQueue, Long> repo;
    private final Emitter emitter;
    private final ObjectMapper objectMapper;

    @SneakyThrows
    public Long createProvidingData(UUID messageId, Map<String, Object> data) {
        var jsonData = objectMapper.writeValueAsString(data);
        var queueMessage = GenericQueue.Factory.create(messageId == null ? UUID.randomUUID() : messageId, jsonData);

        var internalId = repo.create(queueMessage);
        queueMessage.markAsPersisted(internalId, emitter);

        log.debug("Created message [{}] with data {}", internalId, jsonData);

        return internalId;
    }

    public void createWithRandomData(UUID messageId) {
        var faker = new Faker();
        var gotCharacter = faker.gameOfThrones().character();
        var lotrCharacter = faker.lordOfTheRings().character();

        Map<String, Object> data = Map.of("GOT", gotCharacter, "LOTR", lotrCharacter);
        createProvidingData(messageId, data);
    }
}
