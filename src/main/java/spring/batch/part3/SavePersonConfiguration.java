package spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class SavePersonConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    private Map<String, Person> result = new HashMap<>();

    public SavePersonConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job savePersonJob() throws Exception {
        return this.jobBuilderFactory.get("itemCustomJob")
                .incrementer(new RunIdIncrementer())
                .start(this.savePersonStep(null))
                .listener(new SavePersonListener.SavePersonJobExecutionListener())
                .listener(new SavePersonListener.SavePersonAnnotationJobExecutionListener())
                .build();
    }

    @Bean
    @JobScope
    public Step savePersonStep(@Value("#{jobParameters[allow_duplicate]}") String allow_duplicate) throws Exception {
        return this.stepBuilderFactory.get("itemCustomStep")
                .<Person, Person>chunk(10)
                .reader(this.customItemReader())
                .processor(this.customItemProcessor(Boolean.parseBoolean(allow_duplicate)))
                .writer(this.customCompositeItemWriter())
                .build();
    }

    private CompositeItemWriter<Person> customCompositeItemWriter() throws Exception {
        CompositeItemWriter<Person> itemWriter = new CompositeItemWriter<>();
        itemWriter.setDelegates(Arrays.asList(customJpaItemWriter(), customLogItemWriter()));
        return itemWriter;
    }

    private ItemWriter<Person> customJpaItemWriter() throws Exception {
        JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManagerFactory)
                .build();
        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemWriter<Person> customLogItemWriter() throws Exception{
        return items -> items.forEach(p -> log.info("name : {}, age : {}", p.getName(), p.getAge()));
    }

    private ItemProcessor<? super Person, ? extends Person> customItemProcessor(boolean allow_duplicate) {

        if (allow_duplicate) {
            return item -> item;
        } else {
            return item -> {
                if (result.containsKey(item.getName())) {
                    return null;
                } else {
                    result.put(item.getName(), item);
                    return item;
                }
            };
        }
    }

    private FlatFileItemReader<Person> customItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer  = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address");
        tokenizer.setDelimiter(",");
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet ->  new Person(
                fieldSet.readInt(0),
                fieldSet.readString(1),
                fieldSet.readString(2),
                fieldSet.readString(3)));

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("customItemReader")
                .encoding("UTF-8")
                .resource(new FileSystemResource("output/test-output.csv"))
                .linesToSkip(1)
                .lineMapper(lineMapper)
                .build();
        itemReader.afterPropertiesSet();
        return itemReader;
    }
}

