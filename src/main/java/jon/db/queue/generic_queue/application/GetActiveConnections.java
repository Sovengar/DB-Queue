package jon.db.queue.generic_queue.application;

import jon.db.queue.generic_queue.GenericQueueSseEmitter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/generic-queue")
@RequiredArgsConstructor
class GetActiveConnections {
    private final GenericQueueSseEmitter genericQueueSseEmitter;

    @GetMapping("/active-connections")
    public ResponseEntity<Map<String, Integer>> getActiveConnections() {
        return ResponseEntity.ok(Map.of("connections", genericQueueSseEmitter.getActiveConnectionsCount()));
    }
}
