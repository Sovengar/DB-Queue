package jon.db.queue.dead_letter_queue.application;

import jon.db.queue.models.DeadLetterQueue;
import jon.db.queue.store.DLQRepo;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/dead-letter-queue")
@RequiredArgsConstructor
class SearchDLQ {
    private final DLQRepo repo;

    @GetMapping("/search")
    public ResponseEntity<SearchResponse> getAllMessages() {
        var messages = repo.findAll();
        return ResponseEntity.ok(new SearchResponse(messages, "1.0.0", LocalDateTime.now().toString()));
    }

    record SearchResponse(List<DeadLetterQueue> messageId, String version, String retrievedAt) {
    }
}
