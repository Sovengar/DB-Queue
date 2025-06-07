package jon.db.queue.characters.character_queue.infra;

import com.fasterxml.jackson.databind.ObjectMapper;
import jon.db.queue.characters.Character;
import jon.db.queue.characters.character_queue.CharacterQueue;
import jon.db.queue.shared.queue.abstract_queue.QueueRepo;
import jon.db.queue.shared.queue.dead_letter_queue.DeadLetterQueueHandler;
import jon.db.queue.shared.queue.dead_letter_queue.DeadLetterQueue;
import jon.db.queue.shared.Emitter;
import jon.db.queue.characters.CharacterProcessor;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@RequiredArgsConstructor
class CharacterQueueScheduler {
    private final CharacterQueueWorker worker;

    @Scheduled(fixedDelay = 15000)
    public void sendEmailIfNoAccessToQueue() {
        if(!worker.hasAccessToQueue()){
            log.info("Queue is unavailable, sending email to support team...");
        }
    }

    @Scheduled(fixedDelay = 99999)
    public void deleteOldMessages() {
        worker.deleteOldMessages();
    }
}

@Component
@Slf4j
@RequiredArgsConstructor
class CharacterQueuePoller {
    private final CharacterQueueWorker worker;

    private static final int NUMBER_OF_THREADS = 3;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS); // Workers for concurrency

    //Increase the delay based on your needs, i.e., for backpressure
    @Scheduled(fixedDelay = 10000)
    public void pollQueue() {
        log.debug("Polling message queue...");

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final String workerName = "Worker-" + (i + 1);
            executor.submit(() -> worker.processMessages(workerName));
        }
    }

    @Scheduled(fixedDelay = 8000)
    public void sweepPoisonedMessages(){
        worker.processPoisonedMessages();
    } //If the server goes down, when is up will move all messages to DLQ !!!
}

@Service
@Slf4j
@RequiredArgsConstructor
class CharacterQueueWorker {
    private final QueueRepo<CharacterQueue, Long> repo;
    private final CharacterQueueProcessor processor;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Transactional //Has to be here because we are fetching with SKIP LOCKED here
    public void processMessages(String workerName) {
        var queueMessages = fetchMessagesWithLock(workerName);
        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(CharacterQueue::getInternalId).toList());

        for (CharacterQueue msg : queueMessages) {
            processor.processMessageWithErrorHandling(workerName, msg);
        }
    }

    //Don't use, sadly, it doesn't work.
    /**
     * Can't use transactional, because every CompletableFuture executes in a different thread.
     * That means the CompletableFuture thread can't update the msg because it is outside the transaction.
     * That also means that the lock on the original thread is never released. Deadlock.
     */
    public void processMessagesInParallel(String workerName) {
        var queueMessages = fetchMessagesWithLock(workerName);
        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(CharacterQueue::getInternalId).toList());

        // Parallel Processing every message
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (CharacterQueue msg : queueMessages) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> processor.processMessageWithErrorHandling(workerName, msg), executorService);
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
    public void processPoisonedMessages(){
        log.trace("Fetching messages to move to DLQ");
        var messages = fetchPoisonedMessages();

        if(messages.isEmpty()){
            log.trace("No messages to move to DLQ");
            return;
        }

        processor.processPoisonedMessages(messages);
    }

    private List<CharacterQueue> fetchPoisonedMessages() {
        try {
            return repo.lockPoisonedMessages(CharacterQueue.TABLE_NAME);
        } catch (Exception e) {
            log.error("Error retrieving queue messages from DB, abnormal: {}", e.getMessage());
            return List.of();
        }
    }

    List<CharacterQueue> fetchMessagesWithLock(final String workerName) {
        try {
            return repo.lockNextMessages(CharacterQueue.TABLE_NAME, 3, CharacterQueue.MAX_RETRIES);
        } catch (Exception e) {
            log.error("[{}] Error retrieving queue messages from DB, abnormal: {}", workerName, e.getMessage());
            return List.of();
        }
    }

    boolean hasAccessToQueue() {
        try {
            repo.lockNextMessages(CharacterQueue.TABLE_NAME, 3, CharacterQueue.MAX_RETRIES);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    void deleteOldMessages() {
        repo.deleteOldMessages(CharacterQueue.TABLE_NAME);
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class CharacterQueueProcessor {
    private final QueueRepo<CharacterQueue, Long> repo;
    private final Emitter emitter;
    private final CharacterProcessor domainService;
    private final CharacterQueueErrorHandler errorHandler;
    private final ObjectMapper objectMapper;

    void processMessageWithErrorHandling(final String workerName, final CharacterQueue msg) {
        try {
            log.trace("[{}] Processing message {} with data: {}", workerName, msg.getInternalId(), msg.getData());
            processMessage(msg);
        } catch (Exception e) {
            log.error("[{}] Error processing message {}: {}", workerName, msg.getInternalId(), e.getMessage());
            errorHandler.handle(workerName, msg);
        }
    }

    @SneakyThrows
    private void processMessage(final CharacterQueue msg) {
        var character = objectMapper.readValue(msg.getData(), Character.class);

        log.trace("Processing message {} with data: {}", msg.getInternalId(), character);
        domainService.handle(character);
        msg.markAsProcessed(emitter);
        log.trace("Processed message {}", msg.getInternalId());

        repo.update(msg);
    }

    void processPoisonedMessages(List<CharacterQueue> messages){
        log.debug("Moving {} messages {} to DLQ", messages.size(), messages.stream().map(CharacterQueue::getInternalId).toList());
        errorHandler.moveToDLQ(messages);
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class CharacterQueueErrorHandler {
    private final DeadLetterQueueHandler deadLetterQueueHandler;
    private final Emitter emitter;
    private final QueueRepo<CharacterQueue, Long> repo;

    //All has to happen on the same transaction @Transactional(propagation = Propagation.REQUIRES_NEW)
    void handle(final String workerName, final CharacterQueue msg) {
        msg.markAsFailedToProcess(emitter);

        if(!msg.canRetry()){
            log.warn("[{}] Message {} with id {} has reached the maximum number of retries ({}), moving to Dead Letter Queue", workerName, msg.getInternalId(), msg.getMessageId(), CharacterQueue.MAX_RETRIES);
            moveToDLQ(List.of(msg));
        } else {
            repo.update(msg);
        }
    }

    public void moveToDLQ(final List<CharacterQueue> messages) {
        messages.forEach(msg -> {
            log.trace("Moving message {} with id {} to DLQ", msg.getInternalId(), msg.getMessageId());

            var deadLetterQueue = DeadLetterQueue.Factory.create(msg.getMessageId(), msg.getData(), msg.getArrivedAt(), CharacterQueue.TABLE_NAME);
            deadLetterQueueHandler.create(deadLetterQueue);

            msg.markAsDeleted(emitter);
            repo.delete(msg);
        });
    }
}

