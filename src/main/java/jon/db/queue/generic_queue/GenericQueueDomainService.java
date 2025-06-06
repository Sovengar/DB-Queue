package jon.db.queue.generic_queue;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
class GenericQueueDomainService {
    public void handle(Long internalId, String data){
        if(Math.random() < 0.5){
            throw new RuntimeException("Simulating Random error");
        }

        log.trace("Processing message {} with data: {}", internalId, data);

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
}
