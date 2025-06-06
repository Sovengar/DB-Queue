package jon.db.queue.shared.queue.dead_letter_queue;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "dead_letter_queue")
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class DeadLetterQueue {
    @Id
    private UUID messageId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    private String data;
    private LocalDateTime arrivedAt;

    private String fromQueue;

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Factory {
        public static DeadLetterQueue create(UUID messageId, String data, LocalDateTime arrivedAt, String fromQueue) {
            return new DeadLetterQueue(messageId, data, arrivedAt, fromQueue);
        }
    }
}
