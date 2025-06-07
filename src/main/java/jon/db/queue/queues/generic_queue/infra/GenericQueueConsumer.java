package jon.db.queue.queues.generic_queue.infra;

import jon.db.queue.shared.queue.abstract_queue.QueueRepo;
import jon.db.queue.shared.queue.dead_letter_queue.DeadLetterQueueHandler;
import jon.db.queue.shared.queue.dead_letter_queue.DeadLetterQueue;
import jon.db.queue.shared.Emitter;
import jon.db.queue.UseCaseProcessor;
import jon.db.queue.queues.generic_queue.GenericQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
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
class GenericQueueScheduler { //GenericQueuePoller
    private final GenericQueueWorker worker;

    private static final int NUMBER_OF_THREADS = 3;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS); // Workers for concurrency

    //Increase the delay based on your needs, i.e., for backpressure
    @Scheduled(fixedDelay = 10000)
    public void pollQueue() {
        log.debug("Polling message queue...");

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final String workerName = "Worker-" + (i + 1);
            //Change between parallel and not parallel based on your needs.
            executor.submit(() -> worker.processMessages(workerName));
            //executor.submit(() -> worker.processMessagesInParallel(workerName));
        }
    }

    @Scheduled(fixedDelay = 8000)
    public void sweepPoisonedMessages(){
        worker.processPoisonedMessages();
    } //If the server goes down, when is up will move all messages to DLQ !!!

    @Scheduled(fixedDelay = 15000)
    public void sendEmailIfNoAccessToQueue() {
        // SendEmail, Alarming
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class GenericQueueWorker {
    private final GenericQueueProcessor processor;
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);

    @Transactional //Transactional has to be here because we are fetching with SKIP LOCKED here
    public void processMessages(String workerName) {
        var queueMessages = processor.fetchMessagesWithLock(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(GenericQueue::getInternalId).toList());

        for (GenericQueue msg : queueMessages) {
            processor.processMessageWithErrorHandling(workerName, msg);
        }
    }

    //No need for transactional
    public void processMessagesInParallel(String workerName) {
        var queueMessages = processor.fetchMessagesWithLock(workerName);

        log.debug("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(GenericQueue::getInternalId).toList());

        // Parallel Processing every message
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (GenericQueue msg : queueMessages) {
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
        processor.processPoisonedMessages();
    }
}

@Service
@Slf4j
@RequiredArgsConstructor
class GenericQueueProcessor {
    private final QueueRepo<GenericQueue, Long> repo;
    private final Emitter emitter;
    private final UseCaseProcessor domainService;
    private final GenericQueueErrorHandler errorHandler;

    List<GenericQueue> fetchMessagesWithLock(final String workerName) {
        try {
            return repo.lockNextMessages(GenericQueue.TABLE_NAME, 3, GenericQueue.MAX_RETRIES);
        } catch (Exception e) {
            log.error("[{}] Error retrieving queue messages from DB, abnormal: {}", workerName, e.getMessage());
            return List.of();
        }
    }

    void processMessageWithErrorHandling(final String workerName, final GenericQueue msg) {
        try {
            log.trace("[{}] Processing message {} with data: {}", workerName, msg.getInternalId(), msg.getData());
            processMessage(msg);
        } catch (Exception e) {
            log.error("[{}] Error processing message {}: {}", workerName, msg.getInternalId(), e.getMessage());
            errorHandler.handle(workerName, msg);
        }
    }

    private void processMessage(final GenericQueue msg) {
        domainService.handle(msg.getInternalId(), msg.getData());
        msg.markAsProcessed(emitter);
        repo.update(msg);
    }

    void processPoisonedMessages(){
        log.trace("Fetching messages to move to DLQ");
        var messages = fetchPoisonedMessages();

        if(messages.isEmpty()){
            log.trace("No messages to move to DLQ");
            return;
        }

        log.debug("Moving {} messages {} to DLQ", messages.size(), messages.stream().map(GenericQueue::getInternalId).toList());
        errorHandler.moveToDLQ(messages);
    }

    private List<GenericQueue> fetchPoisonedMessages() {
        try {
            return repo.lockPoisonedMessages(GenericQueue.TABLE_NAME);
        } catch (Exception e) {
            log.error("Error retrieving queue messages from DB, abnormal: {}", e.getMessage());
            return List.of();
        }
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class GenericQueueErrorHandler {
    private final DeadLetterQueueHandler deadLetterQueueHandler;
    private final Emitter emitter;
    private final QueueRepo<GenericQueue, Long> repo;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    void handle(final String workerName, final GenericQueue msg) {
        msg.markAsFailedToProcess(emitter);

        if(!msg.canRetry()){
            log.warn("[{}] Message {} with id {} has reached the maximum number of retries ({}), moving to Dead Letter Queue", workerName, msg.getInternalId(), msg.getMessageId(), GenericQueue.MAX_RETRIES);
            moveToDLQ(List.of(msg));
        } else {
            repo.update(msg);
        }
    }

    public void moveToDLQ(final List<GenericQueue> messages) {
        messages.forEach(msg -> {
            log.trace("Moving message {} with id {} to DLQ", msg.getInternalId(), msg.getMessageId());

            var deadLetterQueue = DeadLetterQueue.Factory.create(msg.getMessageId(), msg.getData(), msg.getArrivedAt(), GenericQueue.TABLE_NAME);
            deadLetterQueueHandler.create(deadLetterQueue);

            msg.markAsDeleted(emitter);
            repo.delete(msg);
        });
    }
}

