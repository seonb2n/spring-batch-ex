package spring.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@SpringBootApplication
@EnableBatchProcessing //배치 프로세싱을 하겠다는 @
public class SpringBatchExApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchExApplication.class, args)));
    }

    @Bean
    @Primary
    TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setThreadNamePrefix("batch-thread-");
        taskExecutor.initialize();
        return taskExecutor;
    }

}
