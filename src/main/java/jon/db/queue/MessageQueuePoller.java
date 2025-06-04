package jon.db.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jon.db.queue.models.DeadLetterQueue;
import jon.db.queue.models.QueueMessage;
import jon.db.queue.store.MessageQueueRepo;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@RequiredArgsConstructor
class MessageQueueScheduler {
    private final MessageQueueWorker mqProcessor;
    private final MessageQueueCreator mqCreator;

    private static final int NUMBER_OF_THREADS = 3;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS); // Workers for concurrency

    @Scheduled(fixedDelay = 10000)
    public void pollMessageQueue() {
        log.debug("Polling message queue...");

        //Use the appropriate option, consider making a sleep here as backpressure
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final String workerName = "Worker-" + (i + 1);
            //Change between parallel and not parallel based on your needs.
            //executor.submit(() -> mqProcessor.processMessagesInQueue(workerName));
            executor.submit(() -> mqProcessor.parallelProcessMessagesInQueue(workerName));
        }
    }

    @Scheduled(fixedDelay = 8000)
    public void moveToDLQ(){
        mqProcessor.moveToDeadLetterQueue();
    } //If the server goes down, when is up will move all messages to DLQ !!!

    @Scheduled(fixedDelay = 3000)
    public void simulateInfluxOfMessages() {
        mqCreator.createWithRandomData();
    }

    //TODO
    @Scheduled(fixedDelay = 15000)
    public void sendEmailIfNoAccesToQueue() {

        //TODO SendEmail

    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class MessageQueueWorker {
    private final MessageProcessor messageProcessor;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Transactional //Transactional has to be here because we are fetching with SKIP LOCKED here
    public void processMessagesInQueue(String workerName) {
        var queueMessages = messageProcessor.fetchMessages(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(QueueMessage::getInternalId).toList());

        for (QueueMessage msg : queueMessages) {
            messageProcessor.processMessageWithErrorHandling(workerName, msg);
        }
    }

    //No need for transactional
    public void parallelProcessMessagesInQueue(String workerName) {
        var queueMessages = messageProcessor.fetchMessages(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(QueueMessage::getInternalId).toList());

        // Parallel Processing every message
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (QueueMessage msg : queueMessages) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> messageProcessor.processMessageWithErrorHandling(workerName, msg), executorService);
            futures.add(future);
        }

        try {
            var allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allFutures.get(5, TimeUnit.MINUTES); //Break block after timeout, care
            log.debug("[{}] All messages processed successfully", workerName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[{}] Processing was interrupted", workerName);
        } catch (ExecutionException e) {
            log.error("[{}] Error occurred during parallel processing: {}", workerName, e.getCause().getMessage());
        } catch (TimeoutException e) {
            log.error("[{}] Processing timed out after waiting for 30 minutes", workerName);
        }
    }

    public void moveToDeadLetterQueue(){
        log.debug("Fetching messages to move to DLQ");
        var messages = messageProcessor.fetchPoisonedMessages();

        if(messages.isEmpty()){
            log.debug("No messages to move to DLQ");
            return;
        }

        log.debug("Moving {} messages {} to DLQ", messages.size(), messages.stream().map(QueueMessage::getInternalId).toList());
        messageProcessor.moveToDLQ(messages);
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class MessageProcessor {
    private final MessageQueueRepo repo;
    private final DeadLetterQueueService deadLetterQueueService;
    private final SseEmitterService sseEmitterService;

    List<QueueMessage> fetchMessages(final String workerName) {
        try {
            return repo.lockNextMessages(3, QueueMessage.MAX_RETRIES);
        } catch (Exception e) {
            log.error("[{}] Error retrieving queue messages from DB, abnormal: {}", workerName, e.getMessage());
            return List.of();
        }
    }

    void processMessageWithErrorHandling(final String workerName, final QueueMessage msg) {
        try {
            log.debug("[{}] Processing message {} with data: {}", workerName, msg.getInternalId(), msg.getData());
            processMessage(msg);
        } catch (Exception e) {
            log.error("[{}] Error processing message {}: {}", workerName, msg.getInternalId(), e.getMessage());
            msg.markAsFailedToProcess();

            if(!msg.canRetry()){
                log.warn("[{}] Message {} has reached the maximum number of retries ({}), moving to Dead Letter Queue", workerName, msg.getInternalId(), QueueMessage.MAX_RETRIES);
                moveToDLQ(List.of(msg));
            }

            repo.update(msg);
        }
    }

    List<QueueMessage> fetchPoisonedMessages() {
        try {
            return repo.lockPoisonedMessages();
        } catch (Exception e) {
            log.error("Error retrieving queue messages from DB, abnormal: {}", e.getMessage());
            return List.of();
        }
    }

    private void processMessage(final QueueMessage msg) {
        //TODO, NO ES GENERICO, NO PUEDO LLAMAR CUALQUIER CODIGO, 1 TABLA-QUEUE POR TAREA?

        if(Math.random() < 0.5){
            throw new RuntimeException("Random error");
        }

        log.trace("Processing message {} with data: {}", msg.getInternalId(), msg.getData());

        msg.markAsProcessed();
        sseEmitterService.sendMessageUpdate(msg);
        repo.update(msg);

        var variableExecutionTimeInSeconds = (int) (Math.random() * 10 * 2);
        sleep(variableExecutionTimeInSeconds * 1000); // Simulate a long-running job
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Transactional
    public void moveToDLQ(final List<QueueMessage> messages) {
        messages.forEach(msg -> {
            log.trace("Moving message {} to DLQ", msg.getInternalId());

            var deadLetterQueue = DeadLetterQueue.Factory.create(msg.getMessageId(), msg.getData(), msg.getArrivedAt());
            deadLetterQueueService.create(deadLetterQueue);

            repo.delete(msg);
        });
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class MessageQueueCreator {
    private final ObjectMapper objectMapper;
    private final MessageQueueRepo repo;
    private final SseEmitterService sseEmitterService;

    @SneakyThrows
    public Long createProvidingData(Map<String, Object> data, UUID messageId) {
        var jsonData = objectMapper.writeValueAsString(data);
        var queueMessage = QueueMessage.Factory.create(messageId == null ? UUID.randomUUID() : messageId, jsonData);
        var internalId = repo.create(queueMessage);
        log.debug("Created message [{}] with data {}", internalId, jsonData);

        sseEmitterService.sendMessageToAllClients(queueMessage);

        return internalId;
    }

    public void createWithRandomData() {
        var faker = new Faker();
        var gotCharacter = faker.gameOfThrones().character();
        var lotrCharacter = faker.lordOfTheRings().character();

        Map<String, Object> data = Map.of("GOT", gotCharacter, "LOTR", lotrCharacter);
        createProvidingData(data, UUID.randomUUID());
    }
}
