package jon.db.queue.queues.generic_queue;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jon.db.queue.shared.queue.abstract_queue.QueueEntity;
import jon.db.queue.shared.Emitter;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "generic_queue")
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class GenericQueue implements QueueEntity<Long> {
    public static final int MAX_RETRIES = 3;
    public static final String TABLE_NAME = "generic_queue";

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

    public void markAsFailedToProcess(Emitter emitAction) {
        this.nonTimeoutRetries++;
        emitAction.emitUpdate(this);
    }

    public boolean canRetry() {
        return this.nonTimeoutRetries < MAX_RETRIES;
    }

    //If DB didn't generate pk, then this would be called inside the create method
    public void markAsPersisted(Long internalId, Emitter emitAction) {
        this.internalId = internalId;
        emitAction.emitCreation(this);
    }

    public void markAsProcessed(Emitter emitAction) {
        this.processedAt = LocalDateTime.now();
        emitAction.emitUpdate(this);
    }

    public void markAsDeleted(Emitter emitAction) {
        emitAction.emitDeletion(this);
    }

    public Map<String, Object> transformFieldsToMap(){
        return Map.of(
                "internalId", getInternalId() != null ? getInternalId() : "",
                "messageId", getMessageId(),
                "data", getData(),
                "arrivedAt", getArrivedAt(),
                "nonTimeoutRetries", getNonTimeoutRetries(),
                "processedAt", getProcessedAt() != null ? getProcessedAt().toString() : ""
        );
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Factory {
        public static GenericQueue create(UUID messageId, String data) {
            Long id = null;
            var arrivedAt = LocalDateTime.now();
            var nonTimeoutRetries = 0;
            LocalDateTime processedAt = null;
            return new GenericQueue(id, messageId, data, arrivedAt, nonTimeoutRetries, processedAt);
        }
    }
}
