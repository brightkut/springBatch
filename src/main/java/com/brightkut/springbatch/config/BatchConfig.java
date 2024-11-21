package com.brightkut.springbatch.config;

import com.brightkut.springbatch.entity.Student;
import com.brightkut.springbatch.repository.StudentRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
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
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

    private final StudentRepository studentRepository;
    private final JobRepository jobRepository;
    private PlatformTransactionManager platformTransactionManager;

    public BatchConfig(StudentRepository studentRepository, JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        this.studentRepository = studentRepository;
        this.jobRepository = jobRepository;
        this.platformTransactionManager = platformTransactionManager;
    }

    // we use FlatFileItemReader because this support to read csv file, but it has other reader that you can use
    // to appropriate your source
    @Bean
    public FlatFileItemReader<Student> itemReader(){
        FlatFileItemReader<Student> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/students.csv"));
        itemReader.setName("csvReader");

        //set this because we don't want to read header of csv
        itemReader.setLinesToSkip(1);

        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    @Bean
    public StudentProcessor processor(){
        return new StudentProcessor();
    }

    @Bean
    public RepositoryItemWriter<Student> write(){
        RepositoryItemWriter<Student> writer = new RepositoryItemWriter<>();
        writer.setRepository(studentRepository);
        writer.setMethodName("save");

        return writer;
    }

    @Bean
    public Step importStep(){
        return new StepBuilder("csvImport",jobRepository)
                .<Student,Student>chunk(1000,platformTransactionManager)
                .reader(itemReader())
                .processor(processor())
                .writer(write())
                //Run with taskExecutor = 14s847ms
                // without taskExecutor = 22s302ms
                .taskExecutor(taskExecutor())
                .build();
    }

    // use for parallel running
    //disadvantage is the data in DB is not order.
    @Bean
    public TaskExecutor taskExecutor(){
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(10);

        return simpleAsyncTaskExecutor;
    }

    @Bean
    public Job runJob(){
        return new JobBuilder("importStudents",jobRepository)
                .start(importStep())
                // for case that has multiple step
//                .next()
                .build();
    }

    private LineMapper<Student> lineMapper(){
        DefaultLineMapper<Student> lineMapper = new DefaultLineMapper<>();

        //create line tokenizer
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","age");

        //create field set mapper
        BeanWrapperFieldSetMapper<Student> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Student.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }
}
