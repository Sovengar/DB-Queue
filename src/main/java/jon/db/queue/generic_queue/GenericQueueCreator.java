package jon.db.queue.generic_queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jon.db.queue.models.GenericQueue;
import jon.db.queue.store.GenericQueueRepo;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class GenericQueueCreator {
    private final ObjectMapper objectMapper;
    private final GenericQueueRepo repo;
    private final GenericQueueSseEmitter genericQueueSseEmitter;

    @SneakyThrows
    public Long createProvidingData(Map<String, Object> data, UUID messageId) {
        var jsonData = objectMapper.writeValueAsString(data);
        var queueMessage = GenericQueue.Factory.create(messageId == null ? UUID.randomUUID() : messageId, jsonData);
        var internalId = repo.create(queueMessage);
        log.debug("Created message [{}] with data {}", internalId, jsonData);

        genericQueueSseEmitter.sendMessageToAllClients(queueMessage);

        return internalId;
    }

    public void createWithRandomData(UUID messageId) {
        var faker = new Faker();
        var gotCharacter = faker.gameOfThrones().character();
        var lotrCharacter = faker.lordOfTheRings().character();

        Map<String, Object> data = Map.of("GOT", gotCharacter, "LOTR", lotrCharacter);
        createProvidingData(data, messageId);
    }
}
