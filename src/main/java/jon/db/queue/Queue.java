package jon.db.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Table;
import jon.db.queue.models.QueueMessage;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/queue")
@RequiredArgsConstructor
class MessageQueueHttpController {
    private final QueueRepo repo;
    private final MessageQueueCreator mqCreator;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody Map<String, Object> data) {
        mqCreator.createProvidingData(data);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/locked-count")
    public String getLockedCount() {
        var lockedRows = repo.countLockedRows();
        return String.format("There are %s rows locked", lockedRows);
    }
}

@Component
@Slf4j
@RequiredArgsConstructor
class MessageQueueScheduler {
    private final MessageQueueWorker mqProcessor;
    private final MessageQueueCreator mqCreator;

    private static final int NUMBER_OF_THREADS = 3;
    private final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS); // Workers for concurrency

    @Scheduled(fixedDelay = 8000)
    public void pollMessageQueue() {
        log.info("Polling message queue...");

        //Change to parallelProcessMessageInQueue if you want parallelism
        //Use the appropriate option, consider making a sleep here as backpressure
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final String workerName = "Worker-" + (i + 1);
            //executor.submit(() -> mqProcessor.processMessagesInQueue(workerName));
            executor.submit(() -> mqProcessor.parallelProcessMessagesInQueue(workerName));
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void moveToDLQ(){
        //INSERT ON DLQ AND DELETE FROM QUEUE_MESSAGE

    }

    @Scheduled(fixedDelay = 2000)
    public void simulateInfluxOfMessages() {
        mqCreator.createWithRandomData();
    }

    //TODO
    @Scheduled(fixedDelay = 10000)
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

        log.info("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(QueueMessage::getId).toList());

        for (QueueMessage msg : queueMessages) {
            messageProcessor.processMessageWithErrorHandling(workerName, msg);
        }
    }

    //No need for transactional
    public void parallelProcessMessagesInQueue(String workerName) {
        var queueMessages = messageProcessor.fetchMessages(workerName);

        log.info("[{}] Processing {} messages {}", workerName, queueMessages.size(), queueMessages.stream().map(QueueMessage::getId).toList());

        // Parallel Processing every message
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (QueueMessage msg : queueMessages) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> messageProcessor.processMessageWithErrorHandling(workerName, msg), executorService);
            futures.add(future);
        }

        try {
            var allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allFutures.get(5, TimeUnit.MINUTES); //Break block after timeout, care
            log.info("[{}] All messages processed successfully", workerName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[{}] Processing was interrupted", workerName);
        } catch (ExecutionException e) {
            log.error("[{}] Error occurred during parallel processing: {}", workerName, e.getCause().getMessage());
        } catch (TimeoutException e) {
            log.error("[{}] Processing timed out after waiting for 30 minutes", workerName);
        }
    }

    public void moveToDeadLetterQueue(String workerName){
        log.info("[{}] Moving messages to DLQ", workerName);

        var messages = messageProcessor.fetchPoisonedMessages();

        messageProcessor.moveToDLQ(messages);
    }




}

@Service
@Slf4j
@RequiredArgsConstructor
class MessageProcessor {
    private final QueueRepo repo;

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
            log.info("[{}] Processing message {} with data: {}", workerName, msg.getId(), msg.getData());
            processMessage(msg);
        } catch (Exception e) {
            log.error("[{}] Error processing message {}: {}", workerName, msg.getId(), e.getMessage());
            msg.markAsFailedToProcess();

            if(!msg.canRetry()){
                log.warn("[{}] Message {} has reached the maximum number of retries ({}), moving to Dead Letter Queue", workerName, msg.getId(), QueueMessage.MAX_RETRIES);
                moveToDLQ(List.of(msg));
            }

            repo.update(msg);
        }
    }

    List<QueueMessage> fetchPoisonedMessages() {
        try {
            return repo.lockNextMessages(3, QueueMessage.MAX_RETRIES);
        } catch (Exception e) {
            log.error("Error retrieving queue messages from DB, abnormal: {}", e.getMessage());
            return List.of();
        }
    }

    private void processMessage(final QueueMessage msg) {
        //TODO, NO ES GENERICO, NO PUEDO LLAMAR CUALQUIER CODIGO, 1 TABLA-QUEUE POR TAREA?

        msg.markAsProcessed();
        repo.update(msg);
        sleep(3000); // Simulate a long-running job
        //With 3s the parallel process has time to catch up
        //With 6s the parallel process can't catch up
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void moveToDLQ(final List<QueueMessage> messages) {

        messages.forEach(msg -> {
            //TODO ADD TO DLQ
            //repo.delete(msg);
        });
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class MessageQueueCreator {
    private final ObjectMapper objectMapper;
    private final QueueRepo repo;

    @SneakyThrows
    public void createProvidingData(Map<String, Object> data) {
        var jsonData = objectMapper.writeValueAsString(data);
        var queueMessage = QueueMessage.Factory.create(UUID.randomUUID(), jsonData);
        var messageId = repo.create(queueMessage);
        log.info("Created message [{}] with data {}", messageId, jsonData);
    }

    public void createWithRandomData() {
        var faker = new Faker();
        var gotCharacter = faker.gameOfThrones().character();
        var lotrCharacter = faker.lordOfTheRings().character();

        Map<String, Object> data = Map.of("GOT", gotCharacter, "LOTR", lotrCharacter);
        createProvidingData(data);
    }
}

interface QueueSpringJpaRepo extends JpaRepository<QueueMessage, Long> {
    //TODO TEST
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query(value = """
        SELECT * FROM queue_message  
        WHERE status = 'PENDING' 
        ORDER BY id 
        LIMIT 5 
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<QueueMessage> findNextMessages();
}

interface QueueRepo {
    List<QueueMessage> lockNextMessages(int batchSize, int maxRetries);
    List<QueueMessage> lockPoisonedMessages();
    long countLockedRows();
    Long create(QueueMessage queueMessage);
    void update(QueueMessage queueMessage);
    void delete(QueueMessage queueMessage);
}

@Repository
@Transactional
@RequiredArgsConstructor
@Slf4j
class QueueRepositoryImpl implements QueueRepo {
    @PersistenceContext
    private final EntityManager entityManager;
    private final QueueSpringJpaRepo queueSpringJpaRepo;

    @Override
    public List<QueueMessage> lockNextMessages(int batchSize, int maxRetries) {
        return entityManager
                .createNativeQuery("""
                SELECT * FROM queue_message 
                WHERE processed_at IS NULL
                AND non_timeout_retries <= :maxRetries   
                ORDER BY id ASC
                FETCH FIRST :batchSize ROWS ONLY
                FOR NO KEY UPDATE SKIP LOCKED
                """, QueueMessage.class)
                .setParameter("batchSize", batchSize)
                .setParameter("maxRetries", maxRetries)
                .getResultList();
        //                .createQuery("SELECT q FROM QueueMessage q WHERE q.processedAt IS NULL ORDER BY q.id ASC", QueueMessage.class)
        //                .setMaxResults(batchSize)
        //                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
        //                .setHint("jakarta.persistence.lock.timeout", 0)
    }

    @Override
    public List<QueueMessage> lockPoisonedMessages() {
        return entityManager
                .createNativeQuery("""
            SELECT * FROM queue_message 
            WHERE processed_at IS NULL
            AND arrived_at < :oneHourAgo
            ORDER BY id ASC
            FOR NO KEY UPDATE SKIP LOCKED
            """, QueueMessage.class)
                .setParameter("oneHourAgo", LocalDateTime.now().minusHours(1))
                .getResultList();
    }

    @Override
    public long countLockedRows() {
        String tableName = QueueMessage.class.getAnnotation(Table.class).name();

        String sql = """
            SELECT count(*)
            FROM pg_locks l
            JOIN pg_class t ON l.relation = t.oid
            WHERE t.relname = :tableName
              AND l.mode = 'RowExclusiveLock'
              AND l.granted
        """;

        log.info(sql);

        Object result = entityManager.createNativeQuery(sql)
                .setParameter("tableName", tableName)
                .getSingleResult();

        return ((Number) result).longValue();
    }

    @Override
    public Long create(final QueueMessage queueMessage) {
        var msg = queueSpringJpaRepo.save(queueMessage);
        return msg.getId();
    }

    @Override
    public void update(QueueMessage msg) {
        queueSpringJpaRepo.save(msg);
    }

    @Override
    public void delete(final QueueMessage queueMessage) {
        queueSpringJpaRepo.delete(queueMessage);
    }
}
