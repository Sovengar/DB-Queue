package jon.db.queue.queues;

import jon.db.queue.queues.models.GenericQueue;

//Used for applying Double Dispatch in the model
public interface Emitter {
    void emitUpdate(GenericQueue genericQueue);
    void emitCreation(GenericQueue genericQueue);
    void emitDeletion(GenericQueue genericQueue);
}
