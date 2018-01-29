/**
 * The contents of this file are subject to the Regenstrief Public License
 * Version 1.0 (the "License"); you may not use this file except in compliance with the License.
 * Please contact Regenstrief Institute if you would like to obtain a copy of the license.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) Regenstrief Institute.  All Rights Reserved.
 */
package org.ohdsi.webapi.panacea.repository.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.webapi.panacea.mapper.PanaceaPatientSequenceCountMapper;
import org.ohdsi.webapi.panacea.pojo.PanaceaPatientSequenceCount;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Turn on Configuration and EnableBatchProcessing for using this config!!!
 */
@Configuration
@EnableBatchProcessing
public class PanaceaJobConfiguration {
    
    private static final Log log = LogFactory.getLog(PanaceaJobConfiguration.class);
    
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    //    @Autowired
    //    private PanaceaPatientSequenceItemReader pncPatientSequenceItemReader;
    
    //    @Autowired
    //    private JobBuilderFactory jobBuilders;
    //    
    //    @Autowired
    //    private StepBuilderFactory stepBuilders;
    //    
    //    @Bean
    //    public Job getPanaceaJob() {
    //        return this.jobBuilders.get("panaceaJob").start(step1()).build();
    //    }
    
    //    @Bean
    //    public <T> Job getPanaceaJob(final PanaceaService pncService, final Source source, final PanaceaStudy pncStudy) {
    //        
    //        try {
    //            final JdbcCursorItemReader<PanaceaPatientSequenceCount> itemReader = new JdbcCursorItemReader<PanaceaPatientSequenceCount>();
    //            itemReader.setFetchSize(500);
    //            itemReader.setSaveState(false);
    //            itemReader.setVerifyCursorPosition(false);
    //            itemReader.setSql(pncService.getPanaceaPatientSequenceCountSql(pncStudy.getStudyId()));
    //            itemReader.setDataSource(pncService.getSourceJdbcTemplate(source).getDataSource());
    //            itemReader.setRowMapper(new PanaceaPatientSequenceCountMapper());
    //            itemReader.afterPropertiesSet();
    //            
    //            return this.jobBuilders
    //                    .get("panaceaJob")
    //                    .start(step1())
    //                    .next(
    //                        (this.stepBuilders
    //                                .get("panaceaStep3")
    //                                .transactionManager(pncService.getTransactionTemplate().getTransactionManager())
    //                                //TODO -- change chunk size 5000
    //                                .<PanaceaPatientSequenceCount, PanaceaPatientSequenceCount> chunk(500).reader(itemReader)
    //                                .processor(new PanaceaPatientSequenceItemProcessor())
    //                                .writer(new PanaceaPatientSequenceItemWriter()).build())).build();
    //        } catch (final Throwable t) {
    //            t.printStackTrace();
    //            return null;
    //        }
    //    }
    
    //    @Bean
    //    public Step step1() {
    //        return this.stepBuilders.get("panaceaStep1").tasklet(tasklet1()).build();
    //    }
    
    //    @Bean
    //    public Tasklet tasklet1() {
    //        return new PanaceaTasklet1();
    //    }
    
    //    @Bean
    //    public ItemReader<Student> reader() {
    //        final FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();
    //        //reader.setResource(new ClassPathResource("file:///tmp/student-data.csv"));
    //        reader.setResource(new FileSystemResource("c://tmp//student-data.csv"));
    //        reader.setLineMapper(new DefaultLineMapper<Student>() {
    //            
    //            {
    //                setLineTokenizer(new DelimitedLineTokenizer() {
    //                    
    //                    {
    //                        setNames(new String[] { "stdId", "subMarkOne", "subMarkTwo" });
    //                    }
    //                });
    //                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
    //                    
    //                    {
    //                        setTargetType(Student.class);
    //                    }
    //                });
    //            }
    //        });
    //        return reader;
    //    }
    
    @Bean
    public ItemWriter writer() {
        //        final JdbcBatchItemWriter writer = new JdbcBatchItemWriter();
        final FlatFileItemWriter<String> writer = new FlatFileItemWriter();
        //        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Marksheet>());
        //        writer.setSql("INSERT INTO marksheet (studentId,totalMark) VALUES (:stdId,:totalSubMark)");
        //        writer.setDataSource(dataSource);
        writer.setName("test");
        writer.setResource(new FileSystemResource("c:\\tmp\\output.txt"));
        writer.setLineAggregator(new DelimitedLineAggregator());
        try {
            writer.afterPropertiesSet();
        } catch (final Exception e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
        }
        return writer;
    }
    
    //    
    //    @Bean
    //    public ItemProcessor<Student, String> processor() {
    //        return new StudentItemProcessor();
    //
    //    }
    
    @Bean
    public ItemProcessor<PanaceaPatientSequenceCount, String> pncPatientSequenceCountProcessor() {
        return new PanaceaPatientSequenceCountItemProcessor();
    }
    
    //    @Bean
    //    public Job createMarkSheet(final JobBuilderFactory jobs, final StepBuilderFactory stepBuilderFactory) {
    //        return jobs.get("testStudentJob").flow(step(stepBuilderFactory, reader(), writer(), processor())).end().build();
    //    }
    //    
    //    @Bean
    //    public Step step(final StepBuilderFactory stepBuilderFactory, final ItemReader<Student> reader,
    //                     final ItemWriter<String> writer, final ItemProcessor<Student, String> processor) {
    //        return stepBuilderFactory.get("testStudentStep").<Student, String> chunk(500).reader(reader).processor(processor)
    //                .writer(writer).build();
    //    }
    
    @Bean
    public Job createPanaceaJob(JdbcTemplate jdbcTemplate) {
        final PanaceaPatientSequenceItemReader itemReader = new PanaceaPatientSequenceItemReader();
        itemReader.setFetchSize(500);
        itemReader.setSaveState(false);
        itemReader.setVerifyCursorPosition(false);
        itemReader.setIgnoreWarnings(false);
        //TODO -- moved into reader BeforeStep method
        //itemReader.setSql("select study_id from panacea_study");
        itemReader.setSql("");
        itemReader.setDataSource(jdbcTemplate.getDataSource());
        //itemReader.setDataSource(this.jdbcTemplate.getDataSource());
        itemReader.setRowMapper(new PanaceaPatientSequenceCountMapper());
        try {
            itemReader.afterPropertiesSet();
        } catch (final Exception e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return this.jobBuilderFactory.get("panaceaJob")
                .start(panaceaStep(this.stepBuilderFactory, itemReader, writer(), this.pncPatientSequenceCountProcessor()))
                .build();
    }
    
    @Bean
    public Step panaceaStep(final StepBuilderFactory stepBuilderFactory,
                            final ItemReader<PanaceaPatientSequenceCount> reader, final ItemWriter<String> writer,
                            final ItemProcessor<PanaceaPatientSequenceCount, String> processor) {
        return stepBuilderFactory.get("panaceaStep").<PanaceaPatientSequenceCount, String> chunk(500).reader(reader)
                .processor(processor).writer(writer).build();
    }
}
