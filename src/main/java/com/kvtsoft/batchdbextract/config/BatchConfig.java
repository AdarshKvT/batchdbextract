package com.kvtsoft.batchdbextract.config;


import jakarta.persistence.EntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

//    @Bean
//    public JpaPagingItemReader<Customer> reader() {
//        return new JpaPagingItemReaderBuilder<Customer>()
//                .name("customerReader")
//                .entityManagerFactory(entityManagerFactory)
//                .queryString("SELECT c FROM Customer c")
//                .pageSize(10)
//                .build();
//    }

    @Bean
    public JpaPagingItemReader<Customer> reader() throws Exception {
        log.info("Setting up JpaPagingItemReader to read Customer data...");

        JpaPagingItemReader<Customer> reader = new JpaPagingItemReaderBuilder<Customer>()
                .name("customerReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT c FROM Customer c")
                .pageSize(10)
                .build();

        // Log each Customer object read
        reader.afterPropertiesSet(); // Ensure reader is properly initialized
        reader.setSaveState(true); // Enable saving of state
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Customer> writer() {
        return new FlatFileItemWriterBuilder<Customer>()
                .name("customerWriter")
                .resource(new FileSystemResource("customers.csv"))
                .lineAggregator(new DelimitedLineAggregator<>())
                .delimited()
                .delimiter(",")
                .names(new String[]{"id", "firstName", "lastName", "email"})
                .build();
    }

    @Bean
    public Job exportCustomerJob(JobCompletionNotificationListener listener) throws Exception {
        return new JobBuilder("exportCustomerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() throws Exception {
        return new StepBuilder("step1", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(reader())
                .writer(writer())
                .build();
    }
}
