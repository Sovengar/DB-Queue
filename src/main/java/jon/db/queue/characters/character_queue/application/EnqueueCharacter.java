package jon.db.queue.characters.character_queue.application;

import jon.db.queue.characters.Character;
import jon.db.queue.characters.character_queue.infra.CharacterQueueProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/character-queue")
@RequiredArgsConstructor
class EnqueueCharacter {
    private final CharacterQueueProducer producer;

    @PostMapping
    public ResponseEntity<Void> enqueue(@RequestBody EnqueueRequest request) {
        producer.publish(request.messageId(), request.character());

        return ResponseEntity.ok().build();
    }

    record EnqueueRequest(UUID messageId, Character character) { }
}
