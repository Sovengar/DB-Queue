package jon.db.queue.shared;

import jon.db.queue.characters.character_queue.CharacterQueue;

//Used for applying Double Dispatch in the model
public interface Emitter {
    void emitUpdate(CharacterQueue characterQueue);
    void emitCreation(CharacterQueue characterQueue);
    void emitDeletion(CharacterQueue characterQueue);
}
