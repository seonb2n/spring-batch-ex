package spring.batch;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing //배치 프로세싱을 하겠다는 @
public class SpringBatchExApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchExApplication.class, args);
    }

}
