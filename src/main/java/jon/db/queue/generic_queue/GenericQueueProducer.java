package jon.db.queue.generic_queue;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jon.db.queue.api.QueueRepo;
import jon.db.queue.models.GenericQueue;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class GenericQueueProducer {
    private final GenericMessageCreator genericMessageCreator;

    private static final Random RANDOM = new Random();

    public void publish(final UUID messageId, final Map<String, Object> data){
        genericMessageCreator.createProvidingData(messageId, data);
        log.debug("Published message with id {}", messageId);
    }

    @Scheduled(fixedDelay = 3000)
    public void simulateInfluxOfMessages() {
        var messageId = UUID.randomUUID();

        if (RANDOM.nextInt(20) == 0) { // Probability of 1/20 (5%)
            log.debug("Simulating duplication of messages with id {}", messageId);
            genericMessageCreator.createWithRandomData(messageId);
            genericMessageCreator.createWithRandomData(messageId);
        } else {
            genericMessageCreator.createWithRandomData(messageId);
        }
    }

    @Service
    @RequiredArgsConstructor
    @Slf4j
    public static class GenericMessageCreator {
        //private final GenericQueueRepo repo;
        private final QueueRepo<GenericQueue, Long> repo;
        private final GenericQueueSseEmitter genericQueueSseEmitter;
        private final ObjectMapper objectMapper;

        @SneakyThrows
        public Long createProvidingData(UUID messageId, Map<String, Object> data) {
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
            createProvidingData(messageId, data);
        }
    }
}
