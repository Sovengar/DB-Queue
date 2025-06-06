package jon.db.queue.queues.product_queue;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jon.db.queue.shared.queue.abstract_queue.QueueEntity;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "product_queue")
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class ProductQueue implements QueueEntity<Long> {
    public static final int MAX_RETRIES = 3;
    public static final String TABLE_NAME = "product_queue";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long internalId;

    @Column(unique = true)
    private UUID messageId;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    private String data;
    private LocalDateTime arrivedAt;
    private Integer nonTimeoutRetries;
    private LocalDateTime processedAt;

    public void markAsFailedToProcess() {
        this.nonTimeoutRetries++;
    }

    public boolean canRetry() {
        return this.nonTimeoutRetries < MAX_RETRIES;
    }

    public void markAsProcessed() {
        this.processedAt = LocalDateTime.now();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Factory {
        public static ProductQueue create(UUID messageId, String data) {
            Long id = null;
            var arrivedAt = LocalDateTime.now();
            var nonTimeoutRetries = 0;
            LocalDateTime processedAt = null;
            return new ProductQueue(id, messageId, data, arrivedAt, nonTimeoutRetries, processedAt);
        }
    }
}
