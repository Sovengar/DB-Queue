package jon.db.queue.models;

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
    private LocalDateTime processedAt;

    public void manuallyProcessed() {
        this.processedAt = LocalDateTime.now();
    }

    public void markAsProcessed() {
        this.processedAt = LocalDateTime.now();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Factory {
        public static QueueMessage create(String data, UUID messageId) {
            Long id = null;
            var arrivedAt = LocalDateTime.now();
            var nonTimeoutRetries = 0;
            LocalDateTime processedAt = null;
            return new QueueMessage(id, messageId, data, arrivedAt, nonTimeoutRetries, processedAt);
        }
    }
}
