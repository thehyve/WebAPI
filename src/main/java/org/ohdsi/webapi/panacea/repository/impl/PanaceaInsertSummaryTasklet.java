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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 *
 */
public class PanaceaInsertSummaryTasklet implements Tasklet {
    
    
    private static final Log log = LogFactory.getLog(PanaceaInsertSummaryTasklet.class);
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    private final PanaceaStudy pncStudy;
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaInsertSummaryTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
        PanaceaStudy pncStudy) {
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
            
            final String sourceDialect = (String) jobParams.get("sourceDialect");
            
            /**
             * workaround for sql server/sql render not support any of following up-to-date
             * functionalities for aggregate concatenation strings from multiple rows:
             * "for xml path", CLR, STRING_AGG. (oracel and postgres has it's own functions)
             */
            if ("oracle".equals(sourceDialect) || "postgresql".equals(sourceDialect)) {
                return RepeatStatus.FINISHED;
            }
            
            final String sql = this.getSql(jobParams,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId());
            
            log.debug("PanaceaInsertSummaryTasklet.execute, begin... ");
            
            final List<PncTmpIndvJsn> pncTmpIndvJsnList = this.jdbcTemplate.query(sql, new RowMapper<PncTmpIndvJsn>() {
                
                
                @Override
                public PncTmpIndvJsn mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                    PncTmpIndvJsn ptij = new PncTmpIndvJsn();
                    ptij.setRnum(rs.getFloat("rnum"));
                    ptij.setVersion(rs.getInt("rslt_version"));
                    ptij.setIndvJsn(rs.getString("JSON"));
                    ptij.setLvl(rs.getInt("lvl"));
                    ptij.setLeadLvl(rs.getInt("leadLvl"));
                    ptij.setLagLvl(rs.getInt("lagLvl"));
                    
                    return ptij;
                }
            });
            
            //map all PncTmpIndvJsn to 3 versions: 1, 2 and 3 (filtered version with null in rslt_version column)
            Map<Integer, List<PncTmpIndvJsn>> pncTmpIndvJsnMap = new HashMap<Integer, List<PncTmpIndvJsn>>();
            pncTmpIndvJsnMap.put(new Integer(1), new ArrayList<PncTmpIndvJsn>());
            pncTmpIndvJsnMap.put(new Integer(2), new ArrayList<PncTmpIndvJsn>());
            pncTmpIndvJsnMap.put(new Integer(3), new ArrayList<PncTmpIndvJsn>());
            for (PncTmpIndvJsn indvJsn : pncTmpIndvJsnList) {
                if (indvJsn.getVersion() == null || indvJsn.getVersion() < 1) {
                    pncTmpIndvJsnMap.get(3).add(indvJsn);
                } else if (indvJsn.getVersion().equals(1)) {
                    pncTmpIndvJsnMap.get(1).add(indvJsn);
                } else if (indvJsn.getVersion().equals(2)) {
                    pncTmpIndvJsnMap.get(2).add(indvJsn);
                }
                
            }
            
            List<PncTmpIndvJsn> indvJsnList = pncTmpIndvJsnMap.get(1);
            indvJsnList.sort(new Comparator<PncTmpIndvJsn>() {
                
                
                @Override
                public int compare(PncTmpIndvJsn o1, PncTmpIndvJsn o2) {
                    if (o1 != null && o2 != null) {
                        return Float.compare(o1.getRnum(), o2.getRnum());
                    }
                    return 0;
                }
            });
            attachEndingJSONByLead(indvJsnList);
            String JSON = "";
            for (PncTmpIndvJsn indvJsn : indvJsnList) {
                JSON = JSON.concat(indvJsn.getIndvJsn());
            }
            final String jsonInsertSql = this.getInsertSummarySql(jobParams,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId(), JSON);
            
            int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(jsonInsertSql);
                    
                    return PanaceaInsertSummaryTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            
            //TODO - when have time, need simplify this with a method... instead of repeated version 2 and 3
            indvJsnList = pncTmpIndvJsnMap.get(2);
            indvJsnList.sort(new Comparator<PncTmpIndvJsn>() {
                
                
                @Override
                public int compare(PncTmpIndvJsn o1, PncTmpIndvJsn o2) {
                    if (o1 != null && o2 != null) {
                        return Float.compare(o1.getRnum(), o2.getRnum());
                    }
                    return 0;
                }
            });
            attachEndingJSONByLead(indvJsnList);
            JSON = "";
            for (PncTmpIndvJsn indvJsn : indvJsnList) {
                JSON = JSON.concat(indvJsn.getIndvJsn());
            }
            final String version2UpdateSql = this.getUpdateSummarySql(jobParams,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId(), JSON, 2);
            ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(version2UpdateSql);
                    
                    return PanaceaInsertSummaryTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            
            indvJsnList = pncTmpIndvJsnMap.get(3);
            indvJsnList.sort(new Comparator<PncTmpIndvJsn>() {
                
                
                @Override
                public int compare(PncTmpIndvJsn o1, PncTmpIndvJsn o2) {
                    if (o1 != null && o2 != null) {
                        return Float.compare(o1.getRnum(), o2.getRnum());
                    }
                    return 0;
                }
            });
            attachEndingJSONByLead(indvJsnList);
            JSON = "";
            for (PncTmpIndvJsn indvJsn : indvJsnList) {
                JSON = JSON.concat(indvJsn.getIndvJsn());
            }
            final String version3UpdateSql = this.getUpdateSummarySql(jobParams,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId(), JSON, 3);
            ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
                
                
                @Override
                public int[] doInTransaction(final TransactionStatus status) {
                    
                    final String[] stmts = SqlSplit.splitSql(version3UpdateSql);
                    
                    return PanaceaInsertSummaryTasklet.this.jdbcTemplate.batchUpdate(stmts);
                }
            });
            
            cleanTempData(jobParams, chunkContext);
            
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
     * Refactor for summary generation sql script's lead function
     * 
     * @param indvJsnList
     */
    private void attachEndingJSONByLead(List<PncTmpIndvJsn> indvJsnList) {
        for (PncTmpIndvJsn ptij : indvJsnList) {
            if (ptij != null && ptij.getLeadLvl() != null && ptij.getLeadLvl() > 0) {
                /**
                 * <pre>
                 * for refactor following:
                 *  + CASE WHEN LEAD(connect_by_query.Lvl, 1, 1) OVER (order by connect_by_query.rnum) - connect_by_query.Lvl <= 0
                 *  THEN '}' + replicate(']}', connect_by_query.lvl - LEAD(connect_by_query.Lvl, 1, 1) OVER (order by connect_by_query.rnum)) 
                 *     ELSE ''
                 *  END
                 * </pre>
                 */
                if (ptij.getLeadLvl() - ptij.getLvl() <= 0) {
                    ptij.setIndvJsn(
                        ptij.getIndvJsn().concat("}".concat(StringUtils.repeat("]}", ptij.getLvl() - ptij.getLeadLvl()))));
                }
                
            }
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
        String sql = "select rnum, rslt_version, JSON, lvl, leadLvl, lagLvl FROM @pnc_indv_jsn tab1 \n"
                + "where tab1.job_execution_id = @jobExecId \n" + "and tab1.table_row_id = 1 \n";
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        final String pnc_indv_jsn = (String) jobParams.get("pnc_indv_jsn");
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId",
                "drugConceptId", "sourceId", "pnc_ptstg_ct", "jobExecId", "pnc_indv_jsn" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                drugConceptId, sourceId, pnc_ptstg_ct, jobExecId.toString(), pnc_indv_jsn };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private String getInsertSummarySql(final Map<String, Object> jobParams, final Long jobExecId, String JSON) {
        String sql = "insert into @results_schema.pnc_study_summary (study_id, study_results, last_update_time) \n"
                + "values (@studyId, '@JSON', CURRENT_TIMESTAMP) \n";
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId",
                "drugConceptId", "sourceId", "pnc_ptstg_ct", "jobExecId", "studyId", "JSON" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                drugConceptId, sourceId, pnc_ptstg_ct, jobExecId.toString(), this.pncStudy.getStudyId().toString(), JSON };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private String getUpdateSummarySql(final Map<String, Object> jobParams, final Long jobExecId, String JSON, int version) {
        String sql = "";
        
        if (version == 2) {
            sql = "UPDATE @results_schema.pnc_study_summary \n"
                    + "set study_results_2 = '@JSON', last_update_time = CURRENT_TIMESTAMP \n"
                    + "WHERE study_id = @studyId \n";
        } else if (version == 3) {
            sql = "UPDATE @results_schema.pnc_study_summary \n"
                    + "set study_results_filtered = '@JSON', last_update_time = CURRENT_TIMESTAMP \n"
                    + "WHERE study_id = @studyId \n";
        }
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId",
                "drugConceptId", "sourceId", "pnc_ptstg_ct", "jobExecId", "studyId", "JSON" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                drugConceptId, sourceId, pnc_ptstg_ct, jobExecId.toString(), this.pncStudy.getStudyId().toString(), JSON };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private void cleanTempData(final Map<String, Object> jobParams, final ChunkContext chunkContext) {
        String cleanDataSql = "delete from @pnc_ptsq_ct where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_ptstg_ct where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_tmp_cmb_sq_ct where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_smry_msql_cmb where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_indv_jsn where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_unq_trtmt where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_unq_pth_id where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_smrypth_fltr where job_execution_id = @jobExecId; \n"
                + "delete from @pnc_smry_ancstr where job_execution_id = @jobExecId; \n";
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        
        final String pnc_ptsq_ct = (String) jobParams.get("pnc_ptsq_ct");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        final String pnc_smry_msql_cmb = (String) jobParams.get("pnc_smry_msql_cmb");
        final String pnc_indv_jsn = (String) jobParams.get("pnc_indv_jsn");
        final String pnc_unq_trtmt = (String) jobParams.get("pnc_unq_trtmt");
        final String pnc_unq_pth_id = (String) jobParams.get("pnc_unq_pth_id");
        final String pnc_smrypth_fltr = (String) jobParams.get("pnc_smrypth_fltr");
        final String pnc_smry_ancstr = (String) jobParams.get("pnc_smry_ancstr");
        final String pnc_tmp_cmb_sq_ct = (String) jobParams.get("pnc_tmp_cmb_sq_ct");
        
        final String[] params = new String[] { "cdm_schema", "ohdsi_schema", "results_schema", "studyId", "sourceId",
                "pnc_smry_msql_cmb", "pnc_indv_jsn", "pnc_unq_trtmt", "pnc_unq_pth_id", "pnc_smrypth_fltr",
                "pnc_smry_ancstr", "jobExecId", "pnc_tmp_cmb_sq_ct", "cohort_definition_id", "pnc_ptsq_ct", "pnc_ptstg_ct" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier,
                this.pncStudy.getStudyId().toString(), sourceId, pnc_smry_msql_cmb, pnc_indv_jsn, pnc_unq_trtmt,
                pnc_unq_pth_id, pnc_smrypth_fltr, pnc_smry_ancstr,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId().toString(), pnc_tmp_cmb_sq_ct,
                this.pncStudy.getCohortDefId().toString(), pnc_ptsq_ct, pnc_ptstg_ct };
        
        cleanDataSql = SqlRender.renderSql(cleanDataSql, params, values);
        cleanDataSql = SqlTranslate.translateSql(cleanDataSql, "sql server", sourceDialect, null, resultsTableQualifier);
        final String cleanDataSqlString = cleanDataSql;
        
        int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
            
            
            @Override
            public int[] doInTransaction(final TransactionStatus status) {
                
                final String[] stmts = SqlSplit.splitSql(cleanDataSqlString);
                
                return PanaceaInsertSummaryTasklet.this.jdbcTemplate.batchUpdate(stmts);
            }
        });
    }
    
    class PncTmpIndvJsn {
        
        
        float rnum;
        
        Integer version;
        
        String indvJsn;
        
        Integer lvl, leadLvl, lagLvl;
        
        /**
         * @return the rnum
         */
        public float getRnum() {
            return rnum;
        }
        
        /**
         * @param rnum the rnum to set
         */
        public void setRnum(float rnum) {
            this.rnum = rnum;
        }
        
        /**
         * @return the version
         */
        public Integer getVersion() {
            return version;
        }
        
        /**
         * @param version the version to set
         */
        public void setVersion(int version) {
            this.version = version;
        }
        
        /**
         * @return the indvJsn
         */
        public String getIndvJsn() {
            return indvJsn;
        }
        
        /**
         * @param indvJsn the indvJsn to set
         */
        public void setIndvJsn(String indvJsn) {
            this.indvJsn = indvJsn;
        }
        
        /**
         * @return the lvl
         */
        public Integer getLvl() {
            return lvl;
        }
        
        /**
         * @param lvl the lvl to set
         */
        public void setLvl(Integer lvl) {
            this.lvl = lvl;
        }
        
        /**
         * @return the leadLvl
         */
        public Integer getLeadLvl() {
            return leadLvl;
        }
        
        /**
         * @param leadLvl the leadLvl to set
         */
        public void setLeadLvl(Integer leadLvl) {
            this.leadLvl = leadLvl;
        }
        
        /**
         * @return the lagLvl
         */
        public Integer getLagLvl() {
            return lagLvl;
        }
        
        /**
         * @param lagLvl the lagLvl to set
         */
        public void setLagLvl(Integer lagLvl) {
            this.lagLvl = lagLvl;
        }
        
    }
}
