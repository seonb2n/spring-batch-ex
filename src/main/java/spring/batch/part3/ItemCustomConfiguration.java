package spring.batch.part3;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.IncorrectTokenCountException;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.persistence.EntityManagerFactory;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class ItemCustomConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;

    private Map<String, Person> result = new HashMap<>();

    public ItemCustomConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EntityManagerFactory entityManagerFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.entityManagerFactory = entityManagerFactory;
    }

    @Bean
    public Job itemCustomJob() throws Exception {
        return this.jobBuilderFactory.get("itemCustomJob")
                .incrementer(new RunIdIncrementer())
                .start(this.itemCustomStep())
                .build();
    }

    @Bean
    public Step itemCustomStep() throws Exception {
        return this.stepBuilderFactory.get("itemCustomStep")
                .<Person, Person>chunk(10)
                .reader(this.customItemReader())
                .processor(this.ItemProcessorForSkip("true"))
                .writer(this.customCompositeItemWriter())
                .listener(new SavePersonListener.SavePersonStepExecutionListener())
                .faultTolerant()
                .skip(IncorrectTokenCountException.class)
                .skipLimit(3)
                .retry(NotFoundNameException.class)
                .retryLimit(3)
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

    private ItemProcessor<? super Person, ? extends Person> ItemProcessorForSkip(String allow_duplicate) throws Exception {
        ItemProcessor<? super Person, ? extends Person> duplicateValidationProcessor = customItemProcessor(Boolean.parseBoolean(allow_duplicate));
        ItemProcessor<Person, Person> validationProcessor = item -> {
            if (item.isNotEmptyName()) {
                return item;
            }

            throw new NotFoundNameException();
        };

        CompositeItemProcessor<Person, Person> itemProcessor = new CompositeItemProcessorBuilder()
                .delegates(new PersonValidationRetryProcessor(), validationProcessor, duplicateValidationProcessor)
                .build();

        itemProcessor.afterPropertiesSet();
        return itemProcessor;
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

