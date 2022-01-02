package spring.batch.part6;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import spring.batch.part4_1.UserRepository;

import java.util.HashMap;
import java.util.Map;

public class UserLevelUpPartitioner implements Partitioner {
    private final UserRepository userRepository;

    public UserLevelUpPartitioner(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long minId = userRepository.findMinId();
        long maxId = userRepository.findMaxId();

        long targetSize = (maxId - minId) / gridSize + 1; //5000

        /**
         * partition0 : 1..5000
         * partition1 : 5001...1000
         * ...
         */

        Map<String, ExecutionContext> result = new HashMap<>();

        long number = 0;
        long start = minId;
        long end = start + targetSize - 1;

        while (start <= maxId) {
            ExecutionContext valueExecutionContext = new ExecutionContext();

            result.put("partition" + number, valueExecutionContext);
            if (end >= maxId) {
                end = maxId;
            }

            valueExecutionContext.putLong("minId", start);
            valueExecutionContext.putLong("maxId", end);

            start += targetSize;
            end += targetSize;
            number ++;
        }

        return result;
    }
}
