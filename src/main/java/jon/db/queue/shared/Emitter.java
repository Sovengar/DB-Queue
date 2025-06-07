package jon.db.queue.shared;

import jon.db.queue.queues.generic_queue.GenericQueue;

//Used for applying Double Dispatch in the model
public interface Emitter {
    void emitUpdate(GenericQueue genericQueue);
    void emitCreation(GenericQueue genericQueue);
    void emitDeletion(GenericQueue genericQueue);
}
