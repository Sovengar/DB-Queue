package jon.db.queue.generic_queue;

import jon.db.queue.dead_letter_queue.DeadLetterQueueHandler;
import jon.db.queue.models.DeadLetterQueue;
import jon.db.queue.models.GenericQueue;
import jon.db.queue.store.GenericQueueRepo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
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
class GenericQueueScheduler {
    private final GenericQueueWorker mqProcessor;
    private final GenericQueueCreator mqCreator;

    private static final int NUMBER_OF_THREADS = 3;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS); // Workers for concurrency
    private static final Random RANDOM = new Random();

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

        var messageId = UUID.randomUUID();

        if (RANDOM.nextInt(20) == 0) { // Probability of 1/20 (5%)
            log.debug("Simulating duplication of messages with id {}", messageId);
            mqCreator.createWithRandomData(messageId);
            mqCreator.createWithRandomData(messageId);
        } else {
            mqCreator.createWithRandomData(messageId);
        }
    }

    @Scheduled(fixedDelay = 15000)
    public void sendEmailIfNoAccesToQueue() {
        //TODO SendEmail
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class GenericQueueWorker {
    private final GenericQueueProcessor genericQueueProcessor;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Transactional //Transactional has to be here because we are fetching with SKIP LOCKED here
    public void processMessagesInQueue(String workerName) {
        var queueMessages = genericQueueProcessor.fetchMessages(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(GenericQueue::getInternalId).toList());

        for (GenericQueue msg : queueMessages) {
            genericQueueProcessor.processMessageWithErrorHandling(workerName, msg);
        }
    }

    //No need for transactional
    public void parallelProcessMessagesInQueue(String workerName) {
        var queueMessages = genericQueueProcessor.fetchMessages(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(GenericQueue::getInternalId).toList());

        // Parallel Processing every message
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (GenericQueue msg : queueMessages) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> genericQueueProcessor.processMessageWithErrorHandling(workerName, msg), executorService);
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

    @Transactional
    public void moveToDeadLetterQueue(){
        log.debug("Fetching messages to move to DLQ");
        var messages = genericQueueProcessor.fetchPoisonedMessages();

        if(messages.isEmpty()){
            log.debug("No messages to move to DLQ");
            return;
        }

        log.debug("Moving {} messages {} to DLQ", messages.size(), messages.stream().map(GenericQueue::getInternalId).toList());
        genericQueueProcessor.moveToDLQ(messages);
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class GenericQueueProcessor {
    private final GenericQueueRepo repo;
    private final DeadLetterQueueHandler deadLetterQueueHandler;
    private final GenericQueueSseEmitter genericQueueSseEmitter;

    List<GenericQueue> fetchMessages(final String workerName) {
        try {
            return repo.lockNextMessages(GenericQueue.TABLE_NAME, 3, GenericQueue.MAX_RETRIES);
        } catch (Exception e) {
            log.error("[{}] Error retrieving queue messages from DB, abnormal: {}", workerName, e.getMessage());
            return List.of();
        }
    }

    void processMessageWithErrorHandling(final String workerName, final GenericQueue msg) {
        try {
            log.debug("[{}] Processing message {} with data: {}", workerName, msg.getInternalId(), msg.getData());
            processMessage(msg);
        } catch (Exception e) {
            log.error("[{}] Error processing message {}: {}", workerName, msg.getInternalId(), e.getMessage());
            msg.markAsFailedToProcess();

            if(!msg.canRetry()){
                log.warn("[{}] Message {} with id {} has reached the maximum number of retries ({}), moving to Dead Letter Queue", workerName, msg.getInternalId(), msg.getMessageId(), GenericQueue.MAX_RETRIES);
                moveToDLQ(List.of(msg));
            }

            repo.update(msg);
        }
    }

    List<GenericQueue> fetchPoisonedMessages() {
        try {
            return repo.lockPoisonedMessages(GenericQueue.TABLE_NAME);
        } catch (Exception e) {
            log.error("Error retrieving queue messages from DB, abnormal: {}", e.getMessage());
            return List.of();
        }
    }

    private void processMessage(final GenericQueue msg) {
        if(Math.random() < 0.5){
            throw new RuntimeException("Simulating Random error");
        }

        log.trace("Processing message {} with data: {}", msg.getInternalId(), msg.getData());

        msg.markAsProcessed();
        genericQueueSseEmitter.sendMessageUpdate(msg);
        repo.update(msg);

        var variableExecutionTimeInSeconds = (int) (Math.random() * 10 * 1.5);
        sleep(variableExecutionTimeInSeconds * 1000); // Simulate a long-running job
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void moveToDLQ(final List<GenericQueue> messages) {
        messages.forEach(msg -> {
            log.trace("Moving message {} with id {} to DLQ", msg.getInternalId(), msg.getMessageId());

            var deadLetterQueue = DeadLetterQueue.Factory.create(msg.getMessageId(), msg.getData(), msg.getArrivedAt());
            deadLetterQueueHandler.create(deadLetterQueue);

            genericQueueSseEmitter.sendMessageDelete(msg);
            repo.delete(msg);
        });
    }
}

