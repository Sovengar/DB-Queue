package jon.db.queue.queues.generic_queue.infra;

import jon.db.queue.queues.generic_queue.domain.GenericMessageCreator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
        log.debug("Simulating duplication of messages with id {}", messageId);
        genericMessageCreator.createWithRandomData(messageId);
        genericMessageCreator.createWithRandomData(messageId);
    }
}

