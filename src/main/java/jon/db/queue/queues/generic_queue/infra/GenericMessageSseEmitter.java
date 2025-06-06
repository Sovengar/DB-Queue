package jon.db.queue.queues.generic_queue.infra;

import jon.db.queue.queues.Emitter;
import jon.db.queue.queues.infra.HttpSseEmitter;
import jon.db.queue.queues.models.GenericQueue;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
class GenericMessageSseEmitter implements Emitter {
    private final HttpSseEmitter httpSseEmitter;

    @Override
    public void emitUpdate(final GenericQueue msg) {
        httpSseEmitter.sendMessageUpdated(String.valueOf(msg.getInternalId()), msg.transformFieldsToMap());
    }

    @Override
    public void emitCreation(final GenericQueue msg) {
        httpSseEmitter.sendMessageCreated(String.valueOf(msg.getInternalId()), msg.transformFieldsToMap());
    }

    @Override
    public void emitDeletion(final GenericQueue msg) {
        httpSseEmitter.sendMessageDeleted(String.valueOf(msg.getInternalId()), msg.transformFieldsToMap());
    }
}
