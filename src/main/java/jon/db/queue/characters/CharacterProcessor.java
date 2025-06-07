package jon.db.queue.characters;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class CharacterProcessor {

    public void handle(Character character){
        if(Math.random() < 0.3){
            throw new RuntimeException("Simulating Random error");
        }

        //BIZ LOGIC, USE CASE, HERE

        var variableExecutionTimeInSeconds = (int) (Math.random() * 10 * 1.2);
        sleep(variableExecutionTimeInSeconds * 1000); // Simulate a long-running job
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
