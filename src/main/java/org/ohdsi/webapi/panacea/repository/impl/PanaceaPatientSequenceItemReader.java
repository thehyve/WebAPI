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
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.circe.helper.ResourceHelper;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.database.JdbcCursorItemReader;

/**
 * @param <T>
 */
//public class PanaceaPatientSequenceItemReader<T> implements ItemReader<T> {
//@Component("pncPatientSequenceItemReader")
//@Scope("step")
//@Scope(value = "step")
public class PanaceaPatientSequenceItemReader<PanaceaPatientSequenceCount> extends JdbcCursorItemReader<PanaceaPatientSequenceCount> {// implements StepExecutionListener {

    private static final Log log = LogFactory.getLog(PanaceaPatientSequenceItemReader.class);
    
    //@Value("#{jobParameters['studyId']}")
    private Long studyId;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    //    public PanaceaPatientSequenceItemReader(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
    //        final PanaceaService pncService, final PanaceaStudy pncStudy) {
    //        super();
    //        this.jdbcTemplate = jdbcTemplate;
    //        this.transactionTemplate = transactionTemplate;
    //        this.pncService = pncService;
    //        //        this.pncStudy = pncStudy;
    //        
    //        setupReader();
    //    }
    
    public PanaceaPatientSequenceItemReader() {
        super();
        log.info("PanaceaPatientSequenceItemReader: in constructor");
    }
    
    /**
     * @return the studyId
     */
    public Long getStudyId() {
        return this.studyId;
    }
    
    public void setStudyId(final String studyId) {
        this.studyId = new Long(studyId);
    }
    
    public void setStudyId(final Long studyId) {
        this.studyId = studyId;
    }
    
    //    public void setupReader() {
    //        this.setDataSource(this.jdbcTemplate.getDataSource());
    //        this.setRowMapper(new PanaceaPatientSequenceCountMapper());
    //        
    //        // crucial if using mysql to ensure that results are streamed
    //        this.setFetchSize(Integer.MIN_VALUE);
    //        this.setVerifyCursorPosition(false);
    //        
    //        //final String sql = this.pncService.getPanaceaPatientSequenceCountSql(this.pncStudy.getStudyId());
    //        final String sql = this.pncService.getPanaceaPatientSequenceCountSql(this.studyId);
    //        
    //        this.setSql(sql);
    //        
    //        this.setFetchSize(50000);
    //        this.setSaveState(false);
    //        this.setVerifyCursorPosition(false);
    //        try {
    //            this.afterPropertiesSet();
    //        } catch (final Throwable t) {
    //            t.printStackTrace();
    //        }
    //        //        this.open(this.jobExecution.getExecutionContext());
    //        //        final ExecutionContext executionContext = new ExecutionContext();
    //        //        this.open(executionContext);
    //    }
    
    //    @Override
    //    public void afterPropertiesSet() throws Exception {
    //        setSql(sql);
    //        setRowMapper(rowMapper);
    //        setDataSource(dataSource);
    //        super.afterPropertiesSet();
    //    }
    
    //    @Override
    @BeforeStep
    public void beforeStep(final StepExecution stepExecution)
    
    {
        log.debug("PanaceaPatientSequenceItemReader: in beforeStep");
        log.debug("PanaceaPatientSequenceItemReader: before setting sql = " + this.getSql());
        
        final String studyIdFromJobParam = stepExecution.getJobParameters().getString("studyId");
        
        this.studyId = new Long(studyIdFromJobParam);
        
        //TODO -- testing sql (this works first, then test a fairly real sql)
        this.setSql("select study_id from panacea_study");
        
        /**
         * Fairly real sql for testing for a patient: from getDrugCohortPatientCount.sql: SELECT
         * DISTINCT era.person_id person_id, era.drug_concept_id drug_concept_id,
         * myConcept.concept_name concept_name, era.drug_era_start_date drug_era_start_date,
         * era.drug_era_end_date drug_era_end_date, study.study_id study_id, era.drug_era_end_date -
         * era.drug_era_start_date duration FROM
         * 
         * @cds_schema.drug_era era, (SELECT DISTINCT subject_id, COHORT_START_DATE, cohort_end_date
         *                      FROM @ohdsi_schema.cohort WHERE COHORT_DEFINITION_ID = @cohortDefId
         *                      AND subject_id = 2000000030415658) myCohort, (SELECT concept_name,
         *                      concept_id FROM @cds_schema.concept) myConcept, (SELECT
         *                      cohort_definition_id, study_duration, start_date, end_date, study_id
         *                      FROM @ohdsi_schema.panacea_study WHERE study_id = @studyId) study
         *                      WHERE myCohort.cohort_start_date < era.drug_era_start_date AND
         *                      era.drug_era_start_date < myCohort.cohort_end_date AND
         *                      myCohort.cohort_start_date > study.start_date AND
         *                      myCohort.cohort_start_date < study.end_date AND
         *                      myCohort.cohort_end_date < study.end_date AND
         *                      era.drug_era_start_date > study.start_date AND
         *                      era.drug_era_start_date < study.end_date AND era.drug_era_end_date <
         *                      (era.drug_era_start_date + study.study_duration) AND
         *                      myCohort.subject_id = era.person_id AND drug_concept_id in
         *                      (@drugConceptId) AND era.drug_concept_id = myConcept.concept_id
         *                      ORDER BY person_id, drug_era_start_date
         */
        final String realSql = getRealSql(this.studyId, stepExecution);
        
        this.setSql(realSql);
        
        log.debug("PanaceaPatientSequenceItemReader: Getting studyId =" + this.studyId);
        log.debug("PanaceaPatientSequenceItemReader: sql = " + this.getSql());
        log.debug("PanaceaPatientSequenceItemReader: real sql = " + realSql);
    }
    
    private String getRealSql(final Long studyId, final StepExecution stepExecution) {
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/getDrugCohortPatientCount.sql");
        
        final String cdmTableQualifier = stepExecution.getJobParameters().getString("cds_schema");
        final String resultsTableQualifier = stepExecution.getJobParameters().getString("ohdsi_schema");
        final String cohortDefId = stepExecution.getJobParameters().getString("cohortDefId");
        final String drugConceptId = stepExecution.getJobParameters().getString("drugConceptId");
        final String sourceDialect = stepExecution.getJobParameters().getString("sourceDialect");
        
        final String[] params = new String[] { "cds_schema", "ohdsi_schema", "cohortDefId", "studyId", "drugConceptId" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, cohortDefId, studyId.toString(),
                drugConceptId };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, sourceDialect, sourceDialect);
        
        return sql;
    }
    
    /* (non-Jsdoc)
     * @see org.springframework.batch.core.StepExecutionListener#afterStep(org.springframework.batch.core.StepExecution)
     */
    //    @Override
    public ExitStatus afterStep(final StepExecution stepExecution) {
        // TODO Auto-generated method stub
        return stepExecution.getExitStatus();
    }
}
