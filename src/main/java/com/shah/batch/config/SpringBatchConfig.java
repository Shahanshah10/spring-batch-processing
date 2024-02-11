package com.shah.batch.config;

import com.shah.batch.entity.Customer;
import com.shah.batch.partition.ColumRangePartitioner;
import com.shah.batch.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class SpringBatchConfig {

    private final JobRepository jobRepository;
    private final CustomerWriter customerWriter;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public FlatFileItemReader<Customer> reader(){
        var fileItemReader=new FlatFileItemReader<Customer>();
        fileItemReader.setResource(new FileSystemResource("/Users/zawals/IdeaProjects/spring-batch-processing/src/main/resources/csv/customers.csv"));
        fileItemReader.setName("csvReader");
        fileItemReader.setLinesToSkip(1);
        fileItemReader.setLineMapper(lineMapper());
        return fileItemReader;
    }


    private LineMapper<Customer> lineMapper() {
        var lineMapper=new DefaultLineMapper<Customer>();
        var lineTokenizer =new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");
        var fieldSetMapper=new BeanWrapperFieldSetMapper<Customer>();
        fieldSetMapper.setTargetType(Customer.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public CustomerProcessor processor(){
        return new CustomerProcessor();
    }

//    @Bean
//    public RepositoryItemWriter<Customer> writer(){
//        var writer=new RepositoryItemWriter<Customer>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }


    @Bean
    public ColumRangePartitioner columnRangePartitioner(){
        return new ColumRangePartitioner();
    }

    public PartitionHandler partitionHandler(){
        TaskExecutorPartitionHandler partitionHandler=new TaskExecutorPartitionHandler();
        partitionHandler.setGridSize(4);
        partitionHandler.setTaskExecutor(taskExecutor());
        partitionHandler.setStep(slaveStep());
        return partitionHandler;
    }

    @Bean
    public Step slaveStep(){
        var name="slaveStep";
        var builder=new StepBuilder(name,jobRepository);
        return builder.<Customer, Customer>chunk(250,transactionManager)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .build();
    }

    @Bean
    public Step masterStep(){
        var name="masterSlave";
        var builder=new StepBuilder(name,jobRepository);
        return builder
                .partitioner(slaveStep().getName(),columnRangePartitioner())
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    public Job runJob(){
        var name="importCustomerInformation";
        var builder= new JobBuilder(name,jobRepository);
        return builder.start(masterStep())
                .build();
    }

    // To making whole process async for reading & writing.
//    @Bean
//    public TaskExecutor taskExecutor(){
//        SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor();
//        asyncTaskExecutor.setConcurrencyLimit(10);
//        return asyncTaskExecutor;
//    }

    @Bean
    public TaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor taskExecutor=new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }
}
