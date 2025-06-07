package jon.db.queue.queues.generic_queue.infra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jon.db.queue.queues.generic_queue.GenericQueue;
import jon.db.queue.shared.Emitter;
import jon.db.queue.shared.queue.MessageDuplicatedException;
import jon.db.queue.shared.queue.abstract_queue.QueueRepo;
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
            publishSameMsgTwoTimes(messageId);
        } else {
            genericMessageCreator.createWithRandomData(messageId);
        }
    }

    void publishSameMsgTwoTimes(UUID messageId){
        try {
            genericMessageCreator.createWithRandomData(messageId);
            genericMessageCreator.createWithRandomData(messageId);
        } catch (MessageDuplicatedException e) {
            log.warn("Message with id {} already exists in the queue", messageId);
        }
    }

    @Service
    @RequiredArgsConstructor
    @Slf4j
    static class GenericMessageCreator {
        private final QueueRepo<GenericQueue, Long> repo;
        private final Emitter emitter;
        private final ObjectMapper objectMapper;

        @SneakyThrows
        public Long createProvidingData(UUID messageId, Map<String, Object> data) {
            var jsonData = objectMapper.writeValueAsString(data);
            var queueMessage = GenericQueue.Factory.create(messageId == null ? UUID.randomUUID() : messageId, jsonData);

            var internalId = repo.create(queueMessage);
            queueMessage.markAsPersisted(internalId, emitter);

            //TODO PASAR A DEBUG
            log.trace("Created message [{}] with data {}", internalId, jsonData);

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

