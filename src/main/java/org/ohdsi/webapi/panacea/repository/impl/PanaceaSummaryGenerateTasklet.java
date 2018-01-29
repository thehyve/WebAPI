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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.circe.helper.ResourceHelper;
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
public class PanaceaSummaryGenerateTasklet implements Tasklet {
    
    
    private static final Log log = LogFactory.getLog(PanaceaSummaryGenerateTasklet.class);
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    private final PanaceaStudy pncStudy;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaSummaryGenerateTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
        final PanaceaStudy pncStudy) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
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
            
            final String sql = this.getSql(jobParams,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId());
            
            final int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(sql);
                    
                    return PanaceaSummaryGenerateTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            log.debug("PanaceaSummaryGenerateTasklet execute returned size: " + ret.length);
            
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
    
    private String getSql(final Map<String, Object> jobParams, final Long jobExecId) {
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_smry_msql_cmb = (String) jobParams.get("pnc_smry_msql_cmb");
        final String pnc_indv_jsn = (String) jobParams.get("pnc_indv_jsn");
        final String pnc_unq_trtmt = (String) jobParams.get("pnc_unq_trtmt");
        final String pnc_unq_pth_id = (String) jobParams.get("pnc_unq_pth_id");
        final String pnc_smrypth_fltr = (String) jobParams.get("pnc_smrypth_fltr");
        final String pnc_smry_ancstr = (String) jobParams.get("pnc_smry_ancstr");
        final String pnc_tmp_cmb_sq_ct = (String) jobParams.get("pnc_tmp_cmb_sq_ct");
        
        final String tempTableCreationSummary_oracle = this.getTempTableCreationOracle(jobParams);
        
        String sql = "";
        String insertReplaceCTE_1Sql = "";
        String insertReplaceCTE_2Sql = "";
        String insertReplaceCTE_3Sql = "";
        if ("oracle".equalsIgnoreCase(sourceDialect)) {
            sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/generateSummary.sql");
        } else if ("postgresql".equalsIgnoreCase(sourceDialect)) {
            sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/generateSummary_postgres.sql");
        } else {
            /**
             * default as sql server version
             */
            sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/generateSummary_mssql.sql");
            
            //replace @insertReplaceCTE_1
            insertReplaceCTE_1Sql = getInsertReplaceCTE_1Sql();
            //replace @insertReplaceCTE_2
            insertReplaceCTE_2Sql = getInsertReplaceCTE_2Sql();
            //replace @insertReplaceCTE_3
            insertReplaceCTE_3Sql = getInsertReplaceCTE_3Sql();
        }
        
        final String[] params = new String[] { "cdm_schema", "ohdsi_schema", "results_schema", "cohortDefId", "studyId",
                "drugConceptId", "sourceId", "pnc_smry_msql_cmb", "pnc_indv_jsn", "pnc_unq_trtmt", "pnc_unq_pth_id",
                "pnc_smrypth_fltr", "pnc_smry_ancstr", "tempTableCreationSummary_oracle", "jobExecId", "pnc_tmp_cmb_sq_ct",
                "cohort_definition_id", "insertReplaceCTE_1", "insertReplaceCTE_2", "insertReplaceCTE_3" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                this.pncStudy.getStudyId().toString(), drugConceptId, sourceId, pnc_smry_msql_cmb, pnc_indv_jsn,
                pnc_unq_trtmt, pnc_unq_pth_id, pnc_smrypth_fltr, pnc_smry_ancstr, tempTableCreationSummary_oracle,
                jobExecId.toString(), pnc_tmp_cmb_sq_ct, this.pncStudy.getCohortDefId().toString(), insertReplaceCTE_1Sql,
                insertReplaceCTE_2Sql, insertReplaceCTE_3Sql };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private String getTempTableCreationOracle(final Map<String, Object> jobParams) {
        
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        
        String tempTableCreationOracle = "";
        if ("oracle".equalsIgnoreCase(sourceDialect)) {
            tempTableCreationOracle = ResourceHelper
                    .GetResourceAsString("/resources/panacea/sql/tempTableCreationSummary_oracle.sql");
        } else {
            /**
             * default as sql server version
             */
            tempTableCreationOracle = "\n";
        }
        
        final String[] params = new String[] {};
        final String[] values = new String[] {};
        
        tempTableCreationOracle = SqlRender.renderSql(tempTableCreationOracle, params, values);
        tempTableCreationOracle = SqlTranslate.translateSql(tempTableCreationOracle, "sql server", sourceDialect, null,
            resultsTableQualifier);
        
        return tempTableCreationOracle;
    }
    
    /**
     * generateSummary_mssql.sql replacing string @insertReplaceCTE_1 with level 1~100 hierarchical
     * insert SQL into temp table #replace_rcte as a refactoring of recursive CTE (SQL render and
     * some other DB doesn't support recursive CTE!)
     * 
     * @return
     */
    private String getInsertReplaceCTE_1Sql() {
        String oneInsertSql = "INSERT INTO #replace_rcte \n"
                + "(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, \n"
                + "tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, \n"
                + "tx_avg_frm_strt, lvl, parent_comb) \n"
                + "select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, \n"
                + "N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, \n"
                + "N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb \n" + "from "
                + "@results_schema.pnc_study_summary_path N \n" + "join #replace_rcte X \n"
                + "on X.pnc_stdy_smry_id = N.tx_path_parent_key \n" + "where X.lvl = ";
        String insertSql = "";
        
        /**
         * <pre>
         * INSERT INTO #replace_rcte 
         * (pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
         * tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
         * tx_avg_frm_strt, lvl, parent_comb)
         * select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, 
         * N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, 
         * N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb
         * from ohdsi.ohdsi.pnc_study_summary_path N
         * join #replace_rcte X
         * on X.pnc_stdy_smry_id = N.tx_path_parent_key
         * where X.lvl = 1;
         * -- hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time
         * </pre>
         */
        for (int i = 1; i <= 100; i++) {
            insertSql = insertSql.concat(oneInsertSql).concat(String.valueOf(i)).concat("; \n");
        }
        
        return insertSql;
    }
    
    /**
     * generateSummary_mssql.sql replacing string @insertReplaceCTE_2 with level 1~100 hierarchical
     * insert SQL into temp table #replace_rcte_2 as a refactoring of recursive CTE (SQL render and
     * some other DB doesn't support recursive CTE!)
     * 
     * @return
     */
    private String getInsertReplaceCTE_2Sql() {
        String oneInsertSql = "INSERT INTO #replace_rcte_2 \n"
                + "(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, \n"
                + "tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, \n"
                + "tx_avg_frm_strt, lvl, parent_comb, modified_path, path_unique_treatment)  \n"
                + "select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth,  \n"
                + "N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap,  \n"
                + "N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb,  \n"
                + "X.modified_path+'>' + N.tx_stg_cmb as modified_path,  \n" + "CASE  \n"
                + "  WHEN CHARINDEX(comb.concept_ids + ',', X.path_unique_treatment) > 0  \n"
                + "  or CHARINDEX(N.tx_stg_cmb + '>', X.modified_path) > 0  \n" + "  THEN X.path_unique_treatment  \n"
                + "  ELSE X.path_unique_treatment+','+comb.concept_ids  \n" + "END  \n" + "as path_unique_treatment  \n"
                + "from @results_schema.pnc_study_summary_path N \n" + "join " + "@pnc_smry_msql_cmb comb  \n"
                + "on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id  \n" + "and comb.job_execution_id = @jobExecId \n"
                + "join #replace_rcte_2 X  \n" + "on X.pnc_stdy_smry_id = N.tx_path_parent_key  \n" + "where X.lvl = ";
        String insertSql = "";
        
        /**
         * <pre>
         * INSERT INTO #replace_rcte_2
         * (pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
         * tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
         * tx_avg_frm_strt, lvl, parent_comb, modified_path, path_unique_treatment)
         * select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, 
         * N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, 
         * N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb,
         * X.modified_path+'>' + N.tx_stg_cmb as modified_path,
         * CASE 
         *   WHEN CHARINDEX(comb.concept_ids + ',', X.path_unique_treatment) > 0
         *   or CHARINDEX(N.tx_stg_cmb + '>', X.modified_path) > 0
         *   THEN X.path_unique_treatment
         *   ELSE X.path_unique_treatment+','+comb.concept_ids
         * END
         * as path_unique_treatment
         * from ohdsi.ohdsi.pnc_study_summary_path N
         * join ohdsi.ohdsi.pnc_tmp_smry_msql_cmb comb
         * on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
         * and comb.job_execution_id = 228
         * join #replace_rcte_2 X
         * on X.pnc_stdy_smry_id = N.tx_path_parent_key
         * where X.lvl = 1;
         * -- hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time
         * </pre>
         */
        for (int i = 1; i <= 100; i++) {
            insertSql = insertSql.concat(oneInsertSql).concat(String.valueOf(i)).concat("; \n");
        }
        
        return insertSql;
    }
    
    /**
     * generateSummary_mssql.sql replacing string @insertReplaceCTE_3 with level 1~100 hierarchical
     * insert SQL into temp table #replace_rcte_3 as a refactoring of recursive CTE (SQL render and
     * some other DB doesn't support recursive CTE!)
     * 
     * @return
     */
    private String getInsertReplaceCTE_3Sql() {
        String oneInsertSql = "INSERT INTO #replace_rcte_3 \n"
                + "(pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth,  \n"
                + "tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap,  \n"
                + "tx_avg_frm_strt, lvl, parent_comb,  \n"
                + "gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount) \n"
                + "select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth,  \n"
                + "N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap,  \n"
                + "N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb, \n"
                + "isnull(ROUND(cast(N.tx_stg_avg_gap as float)/cast(N.tx_stg_avg_dr as float) * 100,2),0),  \n"
                + "uniqueConcepts.conceptsName,  \n" + "uniqueConcepts.conceptsArray, uniqueConcepts.concept_count \n"
                + "from @results_schema.pnc_study_summary_path N \n" + "join @pnc_smry_msql_cmb comb \n"
                + "on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id \n" + "and comb.job_execution_id = @jobExecId \n"
                + "join #replace_rcte_3 X \n" + "on X.pnc_stdy_smry_id = N.tx_path_parent_key \n"
                + "join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count  \n"
                + "from @pnc_unq_pth_id where job_execution_id = @jobExecId) uniqueConcepts \n"
                + "on uniqueConcepts.pnc_tx_smry_id = N.pnc_stdy_smry_id \n" + "where X.lvl = ";
        String insertSql = "";
        
        /**
         * <pre>
         * INSERT INTO #replace_rcte_3
         * (pnc_stdy_smry_id, tx_path_parent_key, tx_stg_cmb, tx_stg_cmb_pth, 
         * tx_seq, tx_stg_cnt, tx_stg_percentage, tx_stg_avg_dr, tx_stg_avg_gap, 
         * tx_avg_frm_strt, lvl, parent_comb, 
         * gap_pcnt, uniqueConceptsName, uniqueConceptsArray, uniqueConceptCount)
         * select N.pnc_stdy_smry_id, N.tx_path_parent_key, N.tx_stg_cmb, N.tx_stg_cmb_pth, 
         * N.tx_seq, N.tx_stg_cnt, N.tx_stg_percentage, N.tx_stg_avg_dr, N.tx_stg_avg_gap, 
         * N.tx_avg_frm_strt, (X.lvl + 1) as lvl, X.tx_stg_cmb as parent_comb,
         * isnull(ROUND(cast(N.tx_stg_avg_gap as float)/cast(N.tx_stg_avg_dr as float) * 100,2),0), 
         * uniqueConcepts.conceptsName, 
         * uniqueConcepts.conceptsArray, uniqueConcepts.concept_count
         * from ohdsi.ohdsi.pnc_study_summary_path N
         * join ohdsi.ohdsi.pnc_tmp_smry_msql_cmb comb
         * on N.tx_stg_cmb = comb.pnc_tx_stg_cmb_id
         * and comb.job_execution_id = 262
         * join #replace_rcte_3 X
         * on X.pnc_stdy_smry_id = N.tx_path_parent_key
         * join (select pnc_tx_smry_id, conceptsName, conceptsArray, concept_count 
         * from ohdsi.ohdsi.pnc_tmp_unq_pth_id where job_execution_id = 262) uniqueConcepts
         * on uniqueConcepts.pnc_tx_smry_id = N.pnc_stdy_smry_id     
         * where X.lvl = 1;
         * -- hard code lvl = from 1 to maximum 100 for this, run one by one increasing lvl by 1 each time
         * </pre>
         */
        for (int i = 1; i <= 100; i++) {
            insertSql = insertSql.concat(oneInsertSql).concat(String.valueOf(i)).concat("; \n");
        }
        
        return insertSql;
    }
}
