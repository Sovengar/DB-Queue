package jon.db.queue.api;

import lombok.Getter;

import java.util.UUID;

@Getter
public class MessageDuplicatedException extends RuntimeException {
    private final UUID messageId;

    public MessageDuplicatedException(UUID messageId) {
        super("The message with id " + messageId + " is already on the queue");
        this.messageId = messageId;
    }

    public MessageDuplicatedException(String message) {
        super(message);
        this.messageId = null;
    }
}

