package spring.batch.part4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;

@Configuration
@Slf4j
public class UserGradeConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    public UserGradeConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job userGradeJob() throws Exception {
        return this.jobBuilderFactory.get("userGradeJob")
                .incrementer(new RunIdIncrementer())
                .start(this.saveUserStep())
//                .next(this.userLevelUpStep())
                .build();
    }

    @Bean
    public Step saveUserStep() throws Exception {
        return this.stepBuilderFactory.get("saveUserStep")
                .<User, User>chunk(10)
                .reader(userItemReader())
                .writer(userItemWriter())
                .build();
    }

    private ItemReader<User> userItemReader() throws Exception {
        DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("userName", "totalPurchase");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> new User(
                fieldSet.readString(0),
                fieldSet.readInt(1)));

        FlatFileItemReader<User> itemReader = new FlatFileItemReaderBuilder<User>()
                .name("userItemReader")
                .encoding("UTF-8")
                .resource(new FileSystemResource("output/test-user.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }

    private ItemWriter<User> userItemWriter() throws Exception {
        JpaItemWriter<User> itemWriter = new JpaItemWriterBuilder<User>()
                .entityManagerFactory(entityManagerFactory)
                .build();
        itemWriter.afterPropertiesSet();
        return itemWriter;
    }


/*    @Bean
    public Step userLevelUpStep() {
        return this.stepBuilderFactory.get("userLevelUpStep")
                .chunk(10)
                .reader()
                .processor()
                .writer()
                .build();
    }*/
}
