package jon.db.queue;

import jon.db.queue.api.QueueRepo;
import jon.db.queue.models.AlertQueue;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class QueueService {

    private final QueueRepo<AlertQueue, Long> alertQueueRepo;

    public List<AlertQueue> processAlertMessages() {
        return alertQueueRepo.lockNextMessages("alert_queue", 10, 3);
    }
}

