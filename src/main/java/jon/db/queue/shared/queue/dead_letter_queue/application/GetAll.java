package jon.db.queue.shared.queue.dead_letter_queue.application;

import jon.db.queue.shared.queue.dead_letter_queue.DeadLetterQueue;
import jon.db.queue.shared.queue.dead_letter_queue.DLQRepo;
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
class GetAll {
    private final DLQRepo repo;

    @GetMapping
    public ResponseEntity<Response> getAllMessages() {
        var messages = repo.findAll();
        return ResponseEntity.ok(new Response(messages, "1.0.0", LocalDateTime.now().toString()));
    }

    record Response(List<DeadLetterQueue> messageId, String version, String retrievedAt) {
    }
}
