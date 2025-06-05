package jon.db.queue.generic_queue.application;

import jon.db.queue.models.GenericQueue;
import jon.db.queue.store.GenericQueueRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/generic-queue")
@RequiredArgsConstructor
class CountLockedRows {
    private final GenericQueueRepo repo;

    @GetMapping("/locked-rows")
    public String countLockedRows() {
        var lockedRows = repo.countLockedRows(GenericQueue.TABLE_NAME);
        return String.format("There are %s rows locked", lockedRows);
    }
}

