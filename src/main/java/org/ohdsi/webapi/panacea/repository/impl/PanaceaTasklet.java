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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 *
 */
public class PanaceaTasklet implements Tasklet {
    
    
    private static final Log log = LogFactory.getLog(PanaceaTasklet.class);
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    private PanaceaService pncService;
    
    private PanaceaStudy pncStudy;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
        final PanaceaService pncService, final PanaceaStudy pncStudy) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.pncService = pncService;
        this.pncStudy = pncStudy;
    }
    
    /**
     * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution,
     *      org.springframework.batch.core.scope.context.ChunkContext)
     */
    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        try {
            final Map<String, Object> jobParams = chunkContext.getStepContext().getJobParameters();
            
            final Long jobExecId = chunkContext.getStepContext().getStepExecution().getJobExecution().getId();
            
            final String sql = this.getSql(jobParams, jobExecId);
            
            final int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(sql);
                    
                    return PanaceaTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            log.debug("PanaceaTasklet execute returned size: " + ret.length);
            
            return RepeatStatus.FINISHED;
        } catch (final Exception e) {
            e.printStackTrace();
            //TODO -- stop the job...
            //return RepeatStatus.CONTINUABLE;
            return RepeatStatus.FINISHED;
        } finally {
            //TODO
            final DefaultTransactionDefinition completeTx = new DefaultTransactionDefinition();
            completeTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            final TransactionStatus completeStatus = this.transactionTemplate.getTransactionManager()
                    .getTransaction(completeTx);
            this.transactionTemplate.getTransactionManager().commit(completeStatus);
        }
        
    }
    
    /**
     * @return the jdbcTemplate
     */
    public JdbcTemplate getJdbcTemplate() {
        return this.jdbcTemplate;
    }
    
    /**
     * @param jdbcTemplate the jdbcTemplate to set
     */
    public void setJdbcTemplate(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * @return the transactionTemplate
     */
    public TransactionTemplate getTransactionTemplate() {
        return this.transactionTemplate;
    }
    
    /**
     * @param transactionTemplate the transactionTemplate to set
     */
    public void setTransactionTemplate(final TransactionTemplate transactionTemplate) {
        this.transactionTemplate = transactionTemplate;
    }
    
    /**
     * @return the pncService
     */
    public PanaceaService getPncService() {
        return this.pncService;
    }
    
    /**
     * @param pncService the pncService to set
     */
    public void setPncService(final PanaceaService pncService) {
        this.pncService = pncService;
    }
    
    /**
     * @return the pncStudy
     */
    public PanaceaStudy getPncStudy() {
        return this.pncStudy;
    }
    
    /**
     * @param pncStudy the pncStudy to set
     */
    public void setPncStudy(final PanaceaStudy pncStudy) {
        this.pncStudy = pncStudy;
    }
    
    private String getSql(final Map<String, Object> jobParams, final Long jobExecId) {
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String allConceptIdsStr = (String) jobParams.get("allConceptIdsStr");
        final String procedureConceptId = (String) jobParams.get("procedureConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        //        final String drugEraStudyOptionalDateConstraint = (String) jobParams.get("drugEraStudyOptionalDateConstraint");
        //        final String procedureStudyOptionalDateConstraint = (String) jobParams.get("procedureStudyOptionalDateConstraint");
        final String drugEraStudyOptionalDateConstraint = getDrugEraStudyOptionalDateConstraint(sourceDialect);
        final String procedureStudyOptionalDateConstraint = getProcedureStudyOptionalDateConstraint(sourceDialect);
        final String rowIdString = (String) jobParams.get("rowIdString");
        final String pnc_ptsq_ct = (String) jobParams.get("pnc_ptsq_ct");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        final String pnc_tmp_cmb_sq_ct = (String) jobParams.get("pnc_tmp_cmb_sq_ct");
        
        final String insertFromDrugEra = this.getDrugEraInsertString(jobParams, jobExecId);
        final String insertFromProcedure = this.getProcedureInsertString(jobParams, jobExecId);
        final String insertIntoComboMapString = this.getInsertIntoComboMapString(jobParams);
        final String tempTableCreationOracle = getTempTableCreationOracle(jobParams);
        
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/runPanaceaStudy.sql");
        if ("oracle".equalsIgnoreCase((String) jobParams.get("sourceDialect"))) {
            sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/runPanaceaStudy_oracle.sql");
        }
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId", "studyId",
                "drugConceptId", "sourceId", "procedureConceptId", "drugEraStudyOptionalDateConstraint",
                "procedureStudyOptionalDateConstraint", "allConceptIdsStr", "insertFromDrugEra", "insertFromProcedure",
                "rowIdString", "insertIntoComboMapString", "jobExecId", "tempTableCreationOracle", "pnc_ptsq_ct",
                "pnc_ptstg_ct", "pnc_tmp_cmb_sq_ct", "STUDY_DURATION" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                this.pncStudy.getStudyId().toString(), drugConceptId, sourceId, procedureConceptId,
                drugEraStudyOptionalDateConstraint, procedureStudyOptionalDateConstraint, allConceptIdsStr,
                insertFromDrugEra, insertFromProcedure, rowIdString, insertIntoComboMapString, jobExecId.toString(),
                tempTableCreationOracle, pnc_ptsq_ct, pnc_ptstg_ct, pnc_tmp_cmb_sq_ct,
                this.pncStudy.getStudyDuration().toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private String getDrugEraInsertString(final Map<String, Object> jobParams, final Long jobExecId) {
        String drugEraInsertString = ResourceHelper.GetResourceAsString("/resources/panacea/sql/drugEraInsert.sql");
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String allConceptIdsStr = (String) jobParams.get("allConceptIdsStr");
        final String procedureConceptId = (String) jobParams.get("procedureConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        //        final String drugEraStudyOptionalDateConstraint = (String) jobParams.get("drugEraStudyOptionalDateConstraint");
        //        final String procedureStudyOptionalDateConstraint = (String) jobParams.get("procedureStudyOptionalDateConstraint");
        final String drugEraStudyOptionalDateConstraint = getDrugEraStudyOptionalDateConstraint(sourceDialect);
        final String procedureStudyOptionalDateConstraint = getProcedureStudyOptionalDateConstraint(sourceDialect);
        if (!StringUtils.isEmpty(drugConceptId)) {
            final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId", "studyId",
                    "drugConceptId", "sourceId", "procedureConceptId", "drugEraStudyOptionalDateConstraint",
                    "procedureStudyOptionalDateConstraint", "allConceptIdsStr", "jobExecId", "COHORT_DEFINITION_ID",
                    "STUDY_DURATION" };
            final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier,
                    cohortDefId, this.pncStudy.getStudyId().toString(), drugConceptId, sourceId, procedureConceptId,
                    drugEraStudyOptionalDateConstraint, procedureStudyOptionalDateConstraint, allConceptIdsStr,
                    jobExecId.toString(), this.pncStudy.getCohortDefId().toString(),
                    this.pncStudy.getStudyDuration().toString() };
            
            drugEraInsertString = SqlRender.renderSql(drugEraInsertString, params, values);
            //            drugEraInsertString = SqlTranslate.translateSql(drugEraInsertString, "sql server", sourceDialect, null,
            //                resultsTableQualifier);
        } else {
            return "\n";
        }
        
        return drugEraInsertString;
    }
    
    private String getProcedureInsertString(final Map<String, Object> jobParams, final Long jobExecId) {
        String procedureInsertString = ResourceHelper.GetResourceAsString("/resources/panacea/sql/procedureInsert.sql");
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String allConceptIdsStr = (String) jobParams.get("allConceptIdsStr");
        final String procedureConceptId = (String) jobParams.get("procedureConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        //        final String drugEraStudyOptionalDateConstraint = (String) jobParams.get("drugEraStudyOptionalDateConstraint");
        //        final String procedureStudyOptionalDateConstraint = (String) jobParams.get("procedureStudyOptionalDateConstraint");
        final String drugEraStudyOptionalDateConstraint = getDrugEraStudyOptionalDateConstraint(sourceDialect);
        final String procedureStudyOptionalDateConstraint = getProcedureStudyOptionalDateConstraint(sourceDialect);
        
        if (!StringUtils.isEmpty(procedureConceptId)) {
            final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId", "studyId",
                    "drugConceptId", "sourceId", "procedureConceptId", "drugEraStudyOptionalDateConstraint",
                    "procedureStudyOptionalDateConstraint", "allConceptIdsStr", "jobExecId", "COHORT_DEFINITION_ID",
                    "STUDY_DURATION" };
            final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier,
                    cohortDefId, this.pncStudy.getStudyId().toString(), drugConceptId, sourceId, procedureConceptId,
                    drugEraStudyOptionalDateConstraint, procedureStudyOptionalDateConstraint, allConceptIdsStr,
                    jobExecId.toString(), this.pncStudy.getCohortDefId().toString(),
                    this.pncStudy.getStudyDuration().toString() };
            
            procedureInsertString = SqlRender.renderSql(procedureInsertString, params, values);
            //            procedureInsertString = SqlTranslate.translateSql(procedureInsertString, "sql server", sourceDialect, null,
            //                resultsTableQualifier);
        } else {
            return "\n";
        }
        
        return procedureInsertString;
    }
    
    private String getInsertIntoComboMapString(final Map<String, Object> jobParams) {
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String allConceptIdsStr = (String) jobParams.get("allConceptIdsStr");
        final String procedureConceptId = (String) jobParams.get("procedureConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        //        final String drugEraStudyOptionalDateConstraint = (String) jobParams.get("drugEraStudyOptionalDateConstraint");
        //        final String procedureStudyOptionalDateConstraint = (String) jobParams.get("procedureStudyOptionalDateConstraint");
        final String drugEraStudyOptionalDateConstraint = getDrugEraStudyOptionalDateConstraint(sourceDialect);
        final String procedureStudyOptionalDateConstraint = getProcedureStudyOptionalDateConstraint(sourceDialect);
        
        /**
         * default as "oracle"
         */
        String insertIntoComboMapString = ResourceHelper
                .GetResourceAsString("/resources/panacea/sql/comboMapInsert_oracle.sql");
        
        if ("sql server".equalsIgnoreCase(sourceDialect)) {
            insertIntoComboMapString = ResourceHelper.GetResourceAsString("/resources/panacea/sql/comboMapInsert_mssql.sql");
        } else if ("postgresql".equalsIgnoreCase(sourceDialect)) {
            insertIntoComboMapString = ResourceHelper
                    .GetResourceAsString("/resources/panacea/sql/comboMapInsert_postgres.sql");
        }
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId", "studyId",
                "drugConceptId", "sourceId", "procedureConceptId", "drugEraStudyOptionalDateConstraint",
                "procedureStudyOptionalDateConstraint", "allConceptIdsStr" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                this.pncStudy.getStudyId().toString(), drugConceptId, sourceId, procedureConceptId,
                drugEraStudyOptionalDateConstraint, procedureStudyOptionalDateConstraint, allConceptIdsStr };
        
        insertIntoComboMapString = SqlRender.renderSql(insertIntoComboMapString, params, values);
        //        insertIntoComboMapString = SqlTranslate.translateSql(insertIntoComboMapString, "sql server", sourceDialect, null,
        //            resultsTableQualifier);
        
        return insertIntoComboMapString;
    }
    
    private String getTempTableCreationOracle(final Map<String, Object> jobParams) {
        
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        
        /**
         * default as "oracle"
         */
        String tempTableCreationOracle = ResourceHelper
                .GetResourceAsString("/resources/panacea/sql/tempTableCreation_oracle.sql");
        
        if ("sql server".equalsIgnoreCase(sourceDialect) || "postgresql".equalsIgnoreCase(sourceDialect)) {
            tempTableCreationOracle = "\n";
        }
        
        final String[] params = new String[] {};
        final String[] values = new String[] {};
        
        tempTableCreationOracle = SqlRender.renderSql(tempTableCreationOracle, params, values);
        //        tempTableCreationOracle = SqlTranslate.translateSql(tempTableCreationOracle, "sql server", sourceDialect, null,
        //            resultsTableQualifier);
        
        return tempTableCreationOracle;
    }
    
    private String getDrugEraStudyOptionalDateConstraint(String sourceDialect) {
        String drugEraStudyOptionalDateConstraint = "";
        if (pncStudy.getStartDate() != null) {
            if ("sql server".equalsIgnoreCase(sourceDialect)) {
                drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                        .concat("AND (era.DRUG_ERA_START_DATE > CONVERT(datetime, '" + pncStudy.getStartDate().toString()
                                + "') OR era.DRUG_ERA_START_DATE = CONVERT(datetime, '" + pncStudy.getStartDate().toString()
                                + "')) \n");
            } else {
                drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                        .concat("AND (era.DRUG_ERA_START_DATE > to_date('" + pncStudy.getStartDate().toString()
                                + "', 'yyyy-mm-dd') OR era.DRUG_ERA_START_DATE = to_date('"
                                + pncStudy.getStartDate().toString() + "', 'yyyy-mm-dd')) \n");
            }
        }
        if (pncStudy.getEndDate() != null) {
            if ("sql server".equalsIgnoreCase(sourceDialect)) {
                drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                        .concat("AND (era.DRUG_ERA_START_DATE < CONVERT(datetime, '" + pncStudy.getEndDate().toString()
                                + "') OR era.DRUG_ERA_START_DATE = CONVERT(datetime, '" + pncStudy.getEndDate().toString()
                                + "')) \n");
            } else {
                drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
                        .concat("AND (era.DRUG_ERA_START_DATE < to_date('" + pncStudy.getEndDate().toString()
                                + "', 'yyyy-mm-dd') OR era.DRUG_ERA_START_DATE = to_date('"
                                + pncStudy.getEndDate().toString() + "', 'yyyy-mm-dd')) \n");
            }
        }
        
        return drugEraStudyOptionalDateConstraint;
    }
    
    private String getProcedureStudyOptionalDateConstraint(String sourceDialect) {
        String procedureStudyOptionalDateConstraint = "";
        if (pncStudy.getStartDate() != null) {
            if ("sql server".equalsIgnoreCase(sourceDialect)) {
                procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                        .concat("AND (proc.PROCEDURE_DATE > CONVERT(datetime, '" + pncStudy.getStartDate().toString()
                                + "') OR proc.PROCEDURE_DATE = CONVERT(datetime, '" + pncStudy.getStartDate().toString()
                                + "')) \n");
            } else {
                procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                        .concat("AND (proc.PROCEDURE_DATE > to_date('" + pncStudy.getStartDate().toString()
                                + "', 'yyyy-mm-dd') OR proc.PROCEDURE_DATE = to_date('" + pncStudy.getStartDate().toString()
                                + "', 'yyyy-mm-dd')) \n");
            }
        }
        if (pncStudy.getEndDate() != null) {
            if ("sql server".equalsIgnoreCase(sourceDialect)) {
                procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                        .concat("AND (proc.PROCEDURE_DATE < CONVERT(datetime, '" + pncStudy.getEndDate().toString()
                                + "') OR proc.PROCEDURE_DATE = CONVERT(datetime, '" + pncStudy.getEndDate().toString()
                                + "')) \n");
            } else {
                procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
                        .concat("AND (proc.PROCEDURE_DATE < to_date('" + pncStudy.getEndDate().toString()
                                + "', 'yyyy-mm-dd') OR proc.PROCEDURE_DATE = to_date('" + pncStudy.getEndDate().toString()
                                + "', 'yyyy-mm-dd')) \n");
            }
        }
        
        return procedureStudyOptionalDateConstraint;
    }
}
