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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlSplit;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombinationMap;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
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
public class PanaceaPatientDrugComboTasklet implements Tasklet {
    
    
    private static final Log log = LogFactory.getLog(PanaceaPatientDrugComboTasklet.class);
    
    //private final PanaceaStageCombinationRepository pncStageCombinationRepository;
    
    //private final PanaceaStageCombinationMapRepository pncStageCombinationMapRepository;
    
    private static final int String = 0;
    
    private static final int List = 0;
    
    private JdbcTemplate jdbcTemplate;
    
    private TransactionTemplate transactionTemplate;
    
    // @PersistenceContext
    // @Autowired
    private final EntityManager entityManager;
    
    private final PanaceaStudy pncStudy;
    
    // key-- comboIds concatenated with "|"
    private final Map<String, PanaceaStageCombination> pncStgComboMap = new HashMap<String, PanaceaStageCombination>();
    
    // concept--comboId
    private final Map<String, Long> pncStgSingleConceptComboMap = new HashMap<String, Long>();
    
    private final Comparator patientStageCombinationCountDateComparator = new Comparator<PatientStageCombinationCount>() {
        
        
        @Override
        public int compare(final PatientStageCombinationCount pscc1, final PatientStageCombinationCount pscc2) {
            if ((pscc1 != null) && (pscc2 != null) && (pscc1.getStartDate() != null) && (pscc2.getStartDate() != null)) {
                return pscc1.getStartDate().before(pscc2.getStartDate()) ? -1 : 1;
            }
            return 0;
        }
    };
    
    /**
     * @param jdbcTemplate
     * @param transactionTemplate
     * @param pncService
     * @param pncStudy
     */
    public PanaceaPatientDrugComboTasklet(final JdbcTemplate jdbcTemplate, final TransactionTemplate transactionTemplate,
        final PanaceaStudy pncStudy, final EntityManager em) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.pncStudy = pncStudy;
        //        this.pncStageCombinationRepository = pncStageCombinationRepository;
        //        this.pncStageCombinationMapRepository = pncStageCombinationMapRepository;
        this.entityManager = em;
    }
    
    /**
     * @see org.springframework.batch.core.step.tasklet.Tasklet#execute(org.springframework.batch.core.StepContribution,
     *      org.springframework.batch.core.scope.context.ChunkContext)
     */
    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        try {
            final Map<String, Object> jobParams = chunkContext.getStepContext().getJobParameters();
            
            final List<String> allDistinctPersonId = (List<String>) chunkContext.getStepContext().getJobExecutionContext()
                    .get("allDistinctPersonId");
            String allDistinctPersonIdStr = "";
            
            final int switchWindow = new Integer((String) jobParams.get("switchWindow")).intValue();
            final int ptCountThreshold = 400;
            
            /**
             * Deprecated: OHDSI-75 (removing sequence totally from sql server and support
             * different/dynamic schema/sources). sql server workaround (hibernate does not support
             * sequence for sql server dialect. Cause id always 0 for saved/persistent objects. That
             * cases same session multiple objects with the same id exceptions
             */
            //OHDSI-75
            final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
            final String sourceDialect = (String) jobParams.get("sourceDialect");
            
            String createTempSeqTableSql = "";
            
            //if ("sql server".equalsIgnoreCase(sourceDialect)) {
            createTempSeqTableSql += "create table #_pnc_tmp_mssql_seq_id(nextcombid int); \n";
            //}
            
            final String[] createTempSeqTableParams = new String[] {};
            final String[] createTempSeqTableValues = new String[] {};
            
            createTempSeqTableSql = SqlRender.renderSql(createTempSeqTableSql, createTempSeqTableParams,
                createTempSeqTableValues);
            createTempSeqTableSql = SqlTranslate.translateSql(createTempSeqTableSql, "sql server", sourceDialect, null,
                resultsTableQualifier);
            
            this.batchUpdate(createTempSeqTableSql);
            
            if ((allDistinctPersonId != null) && (allDistinctPersonId.size() > 0)) {
                final Iterator<String> ptIdIter = allDistinctPersonId.iterator();
                
                int ptCount = 0;
                boolean firstId = true;
                while (true) {
                    final String ptId = ptIdIter.next();
                    allDistinctPersonIdStr = firstId ? allDistinctPersonIdStr.concat(ptId)
                            : allDistinctPersonIdStr.concat("," + ptId);
                    firstId = false;
                    ptCount++;
                    if ((ptCount >= ptCountThreshold) || !ptIdIter.hasNext()) {
                        
                        final String sql = this.getSql(jobParams, chunkContext, allDistinctPersonIdStr);
                        
                        log.debug("PanaceaPatientDrugComboTasklet.execute, begin... ");
                        
                        final List<PatientStageCount> patientStageCountList = this.jdbcTemplate.query(sql,
                            new RowMapper<PatientStageCount>() {
                                
                                
                                @Override
                                public PatientStageCount mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                                    final PatientStageCount patientStageCount = new PatientStageCount();
                                    
                                    patientStageCount.setPersonId(rs.getLong("person_id"));
                                    patientStageCount.setCmbId(rs.getLong("cmb_id"));
                                    patientStageCount.setStartDate(rs.getDate("start_date"));
                                    patientStageCount.setEndDate(rs.getDate("end_date"));
                                    
                                    return patientStageCount;
                                }
                            });
                        
                        log.debug(
                            "PanaceaPatientDrugComboTasklet.execute, returned size -- " + patientStageCountList.size());
                        
                        // final List<PatientStageCombinationCount>
                        // calculatedOverlappingPSCCList =
                        // mergeComboOverlapWindow(
                        // patientStageCountList, switchWindow, jobParams);
                        //
                        // persistentPatientStageCombinationCount(calculatedOverlappingPSCCList,
                        // jobParams);
                        
                        final Map<Integer, List<PatientStageCombinationCount>> calculatedOverlappingPSCCMap = mergeComboOverlapWindow(
                            patientStageCountList, switchWindow, jobParams);
                        
                        int[] batchCount = persistentPatientStageCombinationCount(
                            calculatedOverlappingPSCCMap.get(new Integer(1)), jobParams,
                            chunkContext.getStepContext().getStepExecution().getJobExecution().getId());
                        if (batchCount != null) {
                            int count = 0;
                            for (final int cn : batchCount) {
                                count += cn;
                            }
                            log.debug("PanaceaPatientDrugComboTasklet.execute, committed version 1 PSCC - " + count);
                        }
                        
                        batchCount = persistentPatientStageCombinationCount(calculatedOverlappingPSCCMap.get(new Integer(2)),
                            jobParams, chunkContext.getStepContext().getStepExecution().getJobExecution().getId());
                        if (batchCount != null) {
                            int count = 0;
                            for (final int cn : batchCount) {
                                count += cn;
                            }
                            log.debug("PanaceaPatientDrugComboTasklet.execute, committed version 2 PSCC - " + count);
                        }
                        
                        if (ptIdIter.hasNext()) {
                            ptCount = 0;
                            firstId = true;
                            allDistinctPersonIdStr = "";
                        } else {
                            break;
                        }
                    }
                }
            }
            
            return RepeatStatus.FINISHED;
        } catch (final Exception e) {
            e.printStackTrace();
            
            // TODO -- consider this bad? and terminate the job?
            // return RepeatStatus.CONTINUABLE;
            return RepeatStatus.FINISHED;
        } finally {
            // TODO
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
    
    private String getSql(final Map<String, Object> jobParams, final ChunkContext chunkContext,
                          final String allDistinctPersonIdStr) {
        // String sql =
        // ResourceHelper.GetResourceAsString("/resources/panacea/sql/getPersonIds.sql");
        // sql server temp table workaround...
        String sql = "select ptstg.person_id as person_id, ptstg.tx_stg_cmb_id cmb_id, ptstg.stg_start_date start_date, ptstg.stg_end_date end_date "
                + "from @pnc_ptstg_ct  ptstg "
                // + "from #_pnc_ptstg_ct ptstg "
                + "where " + "person_id in (@allDistinctPersonId) and job_execution_id = @jobExecId "
                + "order by person_id, stg_start_date, stg_end_date";
                
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_ptstg_ct = (String) jobParams.get("pnc_ptstg_ct");
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId",
                "drugConceptId", "sourceId", "allDistinctPersonId", "pnc_ptstg_ct", "jobExecId" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                drugConceptId, sourceId, allDistinctPersonIdStr, pnc_ptstg_ct,
                chunkContext.getStepContext().getStepExecution().getJobExecution().getId().toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        return sql;
    }
    
    private Map<Integer, List<PatientStageCombinationCount>> mergeComboOverlapWindow(final List<PatientStageCount> patientStageCountList,
                                                                                     final int switchWindow,
                                                                                     final Map<String, Object> jobParams) {
        if ((patientStageCountList != null) && (patientStageCountList.size() > 0)) {
            final Map<Long, List<PatientStageCombinationCount>> mergedComboPatientMap = new HashMap<Long, List<PatientStageCombinationCount>>();
            
            Long currentPersonId = patientStageCountList.get(0).getPersonId();
            final List<PatientStageCombinationCount> mergedList = new ArrayList<PatientStageCombinationCount>();
            final List<PatientStageCombinationCount> truncatedList = new ArrayList<PatientStageCombinationCount>();
            for (final PatientStageCount psc : patientStageCountList) {
                if (psc.getPersonId().equals(currentPersonId)) {
                    // from same patient
                    while ((truncatedList.size() > 0) && truncatedList.get(0).getStartDate().before(psc.getStartDate())) {
                        popAndMergeList(mergedList, truncatedList, null, switchWindow);
                    }
                    
                    final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                    newPSCC.setPersonId(psc.getPersonId());
                    newPSCC.setComboIds(psc.getCmbId().toString());
                    newPSCC.setStartDate(psc.getStartDate());
                    newPSCC.setEndDate(psc.getEndDate());
                    
                    popAndMergeList(mergedList, truncatedList, newPSCC, switchWindow);
                    
                    if (patientStageCountList.indexOf(psc) == (patientStageCountList.size() - 1)) {
                        // last object in the original list
                        
                        while (truncatedList.size() > 0) {
                            popAndMergeList(mergedList, truncatedList, null, switchWindow);
                        }
                        
                        final List<PatientStageCombinationCount> currentPersonIdMergedList = new ArrayList<PatientStageCombinationCount>();
                        currentPersonIdMergedList.addAll(mergedList);
                        mergedComboPatientMap.put(currentPersonId, currentPersonIdMergedList);
                        
                        mergedList.clear();
                        truncatedList.clear();
                    }
                } else {
                    // read to roll to next patient after popping all from
                    // truncatedList
                    while (truncatedList.size() > 0) {
                        popAndMergeList(mergedList, truncatedList, null, switchWindow);
                    }
                    
                    final List<PatientStageCombinationCount> currentPersonIdMergedList = new ArrayList<PatientStageCombinationCount>();
                    currentPersonIdMergedList.addAll(mergedList);
                    mergedComboPatientMap.put(currentPersonId, currentPersonIdMergedList);
                    
                    mergedList.clear();
                    truncatedList.clear();
                    
                    // first object for next patient
                    currentPersonId = psc.getPersonId();
                    
                    final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                    newPSCC.setPersonId(psc.getPersonId());
                    newPSCC.setComboIds(psc.getCmbId().toString());
                    newPSCC.setStartDate(psc.getStartDate());
                    newPSCC.setEndDate(psc.getEndDate());
                    
                    popAndMergeList(mergedList, truncatedList, newPSCC, switchWindow);
                }
            }
            
            final List<PatientStageCombinationCount> returnPSCCList = new ArrayList<PatientStageCombinationCount>();
            
            this.loadStudyPncStgSingleConceptCombo(jobParams);
            this.loadStudyPncStgCombo(this.pncStudy.getStudyId(), jobParams);
            
            final Map<String, PanaceaStageCombination> newPatientStageCombination = new HashMap<String, PanaceaStageCombination>();
            
            final List<PanaceaStageCombination> newPSCombo = new ArrayList<PanaceaStageCombination>();
            
            for (final Map.Entry<Long, List<PatientStageCombinationCount>> entry : mergedComboPatientMap.entrySet()) {
                for (final PatientStageCombinationCount pscc : entry.getValue()) {
                    if (this.pncStgComboMap.get(pscc.getComboIds()) != null) {
                        // existing combo
                        pscc.setComboIds(this.pncStgComboMap.get(pscc.getComboIds()).getPncTxStgCmbId().toString());
                    } else {
                        // new combo
                        if (!newPatientStageCombination.containsKey(pscc.getComboIds())) {
                            final PanaceaStageCombination newCombo = new PanaceaStageCombination();
                            newCombo.setStudyId(this.pncStudy.getStudyId());
                            
                            final List<PanaceaStageCombinationMap> mapList = new ArrayList<PanaceaStageCombinationMap>();
                            
                            final String[] psccComboStringArray = pscc.getComboIds().split("\\|");
                            final List<String> psccCombos = new ArrayList<String>();
                            psccCombos.addAll(Arrays.asList(psccComboStringArray));
                            for (final String comboString : psccCombos) {
                                final PanaceaStageCombinationMap combMap1 = new PanaceaStageCombinationMap();
                                
                                // TODO -- check on key-concept single concept
                                // generation (delete duplicates when job
                                // starts? may cause other other job run results
                                // fail...)
                                // TODO -- should handle this with lowest
                                // combo_id check
                                // loadStudyPncStgSingleConceptCombo() method,
                                // script using min() function to remove
                                // duplicate single concept combo there
                                final PanaceaStageCombination conceptCombo = this.pncStgComboMap.get(comboString);
                                
                                if ((conceptCombo != null) && (conceptCombo.getCombMapList() != null)
                                        && (conceptCombo.getCombMapList().size() == 1)) {
                                    combMap1.setConceptId(conceptCombo.getCombMapList().get(0).getConceptId());
                                    combMap1.setConceptName(conceptCombo.getCombMapList().get(0).getConceptName());
                                } else {
                                    // TODO -- error logging
                                }
                                
                                mapList.add(combMap1);
                            }
                            
                            newCombo.setCombMapList(mapList);
                            
                            newPSCombo.add(newCombo);
                            newPatientStageCombination.put(pscc.getComboIds(), newCombo);
                        }
                    }
                }
            }
            
            /**
             * Deprecated: OHDSI-75 (removing sequence totally from sql server and support
             * different/dynamic schema/sources). sql server workaround (hibernate does not support
             * sequence for sql server dialect. Cause id always 0 for saved/persistent objects. That
             * cases same session multiple objects with the same id exceptions
             */
            //OHDSI-75
            //            final String sourceDialect = (String) jobParams.get("sourceDialect");
            //            if ("sql server".equalsIgnoreCase(sourceDialect)) {
            //                this.saveComboMap(newPSCombo, jobParams, this.pncStudy.getStudyId());
            //            } else {
            //                this.pncStageCombinationRepository.save(newPSCombo);
            //            }
            //this.pncStageCombinationRepository.save(newPSCombo);
            this.saveComboMap(newPSCombo, jobParams, this.pncStudy.getStudyId());
            
            // TODO -- change this to manipulate the Map?
            this.loadStudyPncStgCombo(this.pncStudy.getStudyId(), jobParams);
            
            final List<PatientStageCombinationCount> collapsedPSCCList = new ArrayList<PatientStageCombinationCount>();
            
            for (final Map.Entry<Long, List<PatientStageCombinationCount>> entry : mergedComboPatientMap.entrySet()) {
                int stage = 1;
                int collapseStage = 1;
                String comboSeq = "";
                for (final PatientStageCombinationCount pscc : entry.getValue()) {
                    final int durationDays = Days
                            .daysBetween(new DateTime(pscc.getStartDate()), new DateTime(pscc.getEndDate())).getDays() + 1;
                    pscc.setDuration(durationDays);
                    pscc.setStage(new Integer(stage));
                    
                    if (this.pncStgComboMap.get(pscc.getComboIds()) != null) {
                        // existing combo
                        pscc.setComboIds(this.pncStgComboMap.get(pscc.getComboIds()).getPncTxStgCmbId().toString());
                    } else {
                        // TODO -- error logging
                    }
                    
                    if (stage == 1) {
                        comboSeq = pscc.getComboIds();
                    } else {
                        comboSeq = comboSeq.concat(">" + pscc.getComboIds());
                    }
                    pscc.setComboSeq(comboSeq);
                    
                    pscc.setResultVerstion(1);
                    pscc.setGapDays(0);
                    
                    returnPSCCList.add(pscc);
                    stage++;
                    
                    if (stage <= 2) {
                        final PatientStageCombinationCount collapsePSCC = new PatientStageCombinationCount();
                        collapsePSCC.setComboIds(pscc.getComboIds());
                        collapsePSCC.setComboSeq(pscc.getComboSeq());
                        collapsePSCC.setStartDate(pscc.getStartDate());
                        collapsePSCC.setEndDate(pscc.getEndDate());
                        collapsePSCC.setDuration(pscc.getDuration());
                        collapsePSCC.setPersonId(pscc.getPersonId());
                        collapsePSCC.setResultVerstion(2);
                        collapsePSCC.setStage(collapseStage);
                        collapsePSCC.setGapDays(0);
                        
                        collapsedPSCCList.add(collapsePSCC);
                        collapseStage++;
                    } else {
                        final PatientStageCombinationCount lastCollapsedItem = collapsedPSCCList
                                .get(collapsedPSCCList.size() - 1);
                        if (lastCollapsedItem != null) {
                            if (pscc.getPersonId().equals(lastCollapsedItem.getPersonId())
                                    && pscc.getComboIds().equals(lastCollapsedItem.getComboIds())) {
                                
                                final int gap = Days.daysBetween(new DateTime(lastCollapsedItem.getEndDate()),
                                    new DateTime(pscc.getStartDate())).getDays();
                                
                                lastCollapsedItem.setEndDate(pscc.getEndDate());
                                
                                final int durationDaysWithGap = Days
                                        .daysBetween(new DateTime(lastCollapsedItem.getStartDate()),
                                            new DateTime(lastCollapsedItem.getEndDate()))
                                        .getDays() + 1;
                                
                                lastCollapsedItem.setDuration(durationDaysWithGap);
                                lastCollapsedItem.setGapDays(lastCollapsedItem.getGapDays() + gap);
                            } else {
                                final PatientStageCombinationCount collapsePSCC = new PatientStageCombinationCount();
                                collapsePSCC.setComboIds(pscc.getComboIds());
                                collapsePSCC.setComboSeq(lastCollapsedItem.getComboSeq() + ">" + pscc.getComboIds());
                                collapsePSCC.setStartDate(pscc.getStartDate());
                                collapsePSCC.setEndDate(pscc.getEndDate());
                                collapsePSCC.setDuration(pscc.getDuration());
                                collapsePSCC.setPersonId(pscc.getPersonId());
                                collapsePSCC.setResultVerstion(2);
                                collapsePSCC.setStage(collapseStage);
                                collapsePSCC.setGapDays(0);
                                
                                collapsedPSCCList.add(collapsePSCC);
                                collapseStage++;
                            }
                        }
                    }
                }
            }
            
            final Map<Integer, List<PatientStageCombinationCount>> psccMapWithVersion = new HashMap<Integer, List<PatientStageCombinationCount>>();
            psccMapWithVersion.put(new Integer(1), returnPSCCList);
            psccMapWithVersion.put(new Integer(2), collapsedPSCCList);
            
            return psccMapWithVersion;
            
        } else {
            // TODO - error logging
            return null;
        }
    }
    
    private void popAndMergeList(final List<PatientStageCombinationCount> mergedList,
                                 final List<PatientStageCombinationCount> truncatedList,
                                 final PatientStageCombinationCount newConstructedPSCC, final int switchWindow) {
        if ((mergedList != null) && (truncatedList != null)) {
            PatientStageCombinationCount poppingPSCC = null;
            boolean newPSCCFromOriginalList = false;
            if (newConstructedPSCC == null) {
                poppingPSCC = truncatedList.get(0);
            } else {
                poppingPSCC = newConstructedPSCC;
                newPSCCFromOriginalList = true;
            }
            
            if (mergedList.size() > 0) {
                // mergedList has elements
                final PatientStageCombinationCount lastMergedPSCC = mergedList.get(mergedList.size() - 1);
                
                if (lastMergedPSCC.getStartDate().after(poppingPSCC.getStartDate())) {
                    
                    log.error("Error in popAndMergeList -- starting date wrong in popAndMergeList");
                }
                
                if (poppingPSCC.getStartDate().before(lastMergedPSCC.getEndDate())) {
                    // overlapping
                    
                    if (poppingPSCC.getEndDate().before(lastMergedPSCC.getEndDate())) {
                        // popping time window is "within" last merged object
                        
                        final int overlappingDays = Days.daysBetween(new DateTime(poppingPSCC.getStartDate()),
                            new DateTime(poppingPSCC.getEndDate())).getDays();
                        
                        if (overlappingDays >= (switchWindow - 1)) {
                            final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                            newPSCC.setPersonId(poppingPSCC.getPersonId());
                            newPSCC.setComboIds(lastMergedPSCC.getComboIds());
                            newPSCC.setStartDate(poppingPSCC.getEndDate());
                            newPSCC.setEndDate(lastMergedPSCC.getEndDate());
                            
                            poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                            
                            lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                            
                            // TODO - verify this more!!!
                            if (lastMergedPSCC.getStartDate().equals(lastMergedPSCC.getEndDate())) {
                                mergedList.remove(mergedList.size() - 1);
                            }
                            /**
                             * see java doc for method combinationSplit()
                             */
                            /*
                             * else { final List<PatientStageCombinationCount>
                             * splittedEarlyMergedList = combinationSplit(
                             * lastMergedPSCC, switchWindow); if
                             * (splittedEarlyMergedList != null) {
                             * mergedList.remove(mergedList.size() - 1);
                             * mergedList.addAll(splittedEarlyMergedList); } }
                             */
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                            
                            /**
                             * see java doc for combinationSplit()
                             */
                            /*
                             * final List<PatientStageCombinationCount>
                             * splittedNewList = combinationSplit(newPSCC,
                             * switchWindow); if (splittedNewList != null) {
                             * truncatedList.addAll(splittedNewList); } else {
                             * truncatedList.add(newPSCC); }
                             */
                            
                            truncatedList.add(newPSCC);
                            
                            Collections.sort(truncatedList, this.patientStageCombinationCountDateComparator);
                        } else {
                            // no overlapping, just pop
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                            
                        }
                    } else if (poppingPSCC.getEndDate().after(lastMergedPSCC.getEndDate())) {
                        // popping object end date is after last merged object
                        
                        final int overlappingDays = Days.daysBetween(new DateTime(poppingPSCC.getStartDate()),
                            new DateTime(lastMergedPSCC.getEndDate())).getDays();
                        
                        if (overlappingDays >= (switchWindow - 1)) {
                            final PatientStageCombinationCount newPSCC = new PatientStageCombinationCount();
                            newPSCC.setPersonId(poppingPSCC.getPersonId());
                            newPSCC.setComboIds(poppingPSCC.getComboIds());
                            newPSCC.setStartDate(lastMergedPSCC.getEndDate());
                            newPSCC.setEndDate(poppingPSCC.getEndDate());
                            
                            poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                            poppingPSCC.setEndDate(lastMergedPSCC.getEndDate());
                            
                            lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                            
                            // TODO - verify this more!!!
                            if (lastMergedPSCC.getStartDate().equals(lastMergedPSCC.getEndDate())) {
                                mergedList.remove(mergedList.size() - 1);
                            }
                            /**
                             * see java doc for combinationSplit()
                             */
                            /*
                             * else { final List<PatientStageCombinationCount>
                             * splittedEarlyMergedList = combinationSplit(
                             * lastMergedPSCC, switchWindow); if
                             * (splittedEarlyMergedList != null) {
                             * mergedList.remove(mergedList.size() - 1);
                             * mergedList.addAll(splittedEarlyMergedList); } }
                             */
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                            
                            /**
                             * see java doc for combinationSplit()
                             */
                            /*
                             * final List<PatientStageCombinationCount>
                             * splittedNewList = combinationSplit(newPSCC,
                             * switchWindow); if (splittedNewList != null) {
                             * truncatedList.addAll(splittedNewList); } else {
                             * truncatedList.add(newPSCC); }
                             */
                            truncatedList.add(newPSCC);
                            
                            Collections.sort(truncatedList, this.patientStageCombinationCountDateComparator);
                        } else {
                            // no overlapping, just pop
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                            
                        }
                        
                    } else if (poppingPSCC.getEndDate().equals(lastMergedPSCC.getEndDate())) {
                        // popping object end date is the same as last merged
                        // object
                        
                        final int overlappingDays = Days.daysBetween(new DateTime(poppingPSCC.getStartDate()),
                            new DateTime(lastMergedPSCC.getEndDate())).getDays();
                        
                        if (overlappingDays >= (switchWindow - 1)) {
                            
                            poppingPSCC.setComboIds(mergeComboIds(lastMergedPSCC, poppingPSCC));
                            
                            lastMergedPSCC.setEndDate(poppingPSCC.getStartDate());
                            
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                        } else {
                            // no overlapping, just pop
                            mergedList.add(poppingPSCC);
                            
                            if (!newPSCCFromOriginalList) {
                                truncatedList.remove(0);
                            }
                            
                        }
                    }
                } else {
                    // no overlapping, just pop
                    mergedList.add(poppingPSCC);
                    
                    if (!newPSCCFromOriginalList) {
                        truncatedList.remove(0);
                    }
                }
            } else {
                // mergedList has no elements, just add the first one
                mergedList.add(poppingPSCC);
                
                // TODO -- check if still needed
                if (!newPSCCFromOriginalList) {
                    truncatedList.remove(0);
                }
            }
        } else {
            // TODO -- error logging
        }
    }
    
    private String mergeComboIds(final PatientStageCombinationCount pscc1, final PatientStageCombinationCount pscc2) {
        
        if ((pscc1 != null) && (pscc2 != null) && (pscc1.getComboIds() != null) && (pscc2.getComboIds() != null)) {
            final String[] pscc1ComboStringArray = pscc1.getComboIds().split("\\|");
            final List<String> psccCombos = new ArrayList<String>();
            psccCombos.addAll(Arrays.asList(pscc1ComboStringArray));
            
            final String[] pscc2ComboStringArray = pscc2.getComboIds().split("\\|");
            psccCombos.addAll(Arrays.asList(pscc2ComboStringArray));
            
            final Set<String> comboSet = new HashSet<String>(psccCombos);
            
            psccCombos.clear();
            psccCombos.addAll(comboSet);
            
            Collections.sort(psccCombos);
            
            return StringUtils.join(psccCombos, "|");
        }
        
        return null;
    }
    
    /**
     * This is to split combination PatientStageCombinationCount. For operating switch window
     * parameter and split old combined combos. May not need it for I think as long the combination
     * has bee combined, basically meaning they overlaps for long enough. When new drug comes in and
     * overlapping with the combined windows, the rest of the window (truncated part and the part
     * before current combination start date) should qualify the switch window too.
     * 
     * <pre>
     * 
     * For example:
     * 
     * A|B
     *  time1       time2
     *  l____________l
     * 
     * A|B|C
     *      time3       time4
     *      l____________l
     * 
     *  When C comes in and trucate combination A|B. The time between time1 and time3 should be considered
     *  as "qualified" combination too because even it's trunkcated by C, A and B's actually length is 
     *  between time1 and time2. So A and B should be considered as taken together.
     *  
     *  The same applies to following between time4 and time2 (A and B taking together):
     * 
     * A|B
     *  time1             time2
     *  l__________________l
     * 
     * A|B|C
     *      time3    time4
     *      l_______l
     * </pre>
     * 
     * @param pscc
     * @param switchWindow
     * @return
     */
    private List<PatientStageCombinationCount> combinationSplit(final PatientStageCombinationCount pscc,
                                                                final int switchWindow) {
        if (pscc != null) {
            
            if ((pscc.getStartDate() != null) && (pscc.getEndDate() != null)) {
                final int overlappingDays = Days
                        .daysBetween(new DateTime(pscc.getStartDate()), new DateTime(pscc.getEndDate())).getDays();
                
                if ((pscc.getComboIds() != null) && pscc.getComboIds().contains("|")
                        && (overlappingDays < (switchWindow - 1))) {
                    final String[] psccComboStringArray = pscc.getComboIds().split("\\|");
                    final List<String> psccCombos = new ArrayList<String>();
                    psccCombos.addAll(Arrays.asList(psccComboStringArray));
                    
                    final List<PatientStageCombinationCount> psccList = new ArrayList<PatientStageCombinationCount>();
                    for (final String comboID : psccCombos) {
                        final PatientStageCombinationCount returnPscc = new PatientStageCombinationCount();
                        returnPscc.setPersonId(pscc.getPersonId());
                        returnPscc.setComboIds(comboID);
                        returnPscc.setStartDate(pscc.getStartDate());
                        returnPscc.setEndDate(pscc.getEndDate());
                        
                        psccList.add(returnPscc);
                    }
                    
                    return psccList;
                }
            }
            // TODO - verify this as returned value
            // final List<PatientStageCombinationCount> psccList = new
            // ArrayList<PatientStageCombinationCount>();
            // psccList.add(pscc);
            // return psccList;
            return null;
        } else {
            return null;
        }
    }
    
    private int[] persistentPatientStageCombinationCount(final List<PatientStageCombinationCount> psccList,
                                                         final Map<String, Object> jobParams, final Long jobExecId) {
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/insertPatientStageComboSequence.sql");
        
        final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String cohortDefId = (String) jobParams.get("cohortDefId");
        final String drugConceptId = (String) jobParams.get("drugConceptId");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final String sourceId = (String) jobParams.get("sourceId");
        final String pnc_tmp_cmb_sq_ct = (String) jobParams.get("pnc_tmp_cmb_sq_ct");
        
        final String[] params = new String[] { "cdm_schema", "results_schema", "ohdsi_schema", "cohortDefId",
                "drugConceptId", "sourceId", "pnc_tmp_cmb_sq_ct", "jobExecId" };
        final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, resultsTableQualifier, cohortDefId,
                drugConceptId, sourceId, pnc_tmp_cmb_sq_ct, jobExecId.toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        /*
         * final String[] sqlStatements = SqlSplit.splitSql(sql);
         * 
         * final int[] updateCounts = this.jdbcTemplate.batchUpdate(sql, new
         * BatchPreparedStatementSetter() {
         * 
         * @Override public void setValues(final PreparedStatement ps, final int
         * i) throws SQLException { ps.setLong(1, 123); ps.setString(2, "dfad");
         * ps.setDate(3, null); ps.setDate(4, null); ps.setInt(5, 1); }
         * 
         * @Override public int getBatchSize() { return psccList.size(); } });
         */
        // TODO -- commit!!!!!
        
        try {
            return this.batchInsertPSCC(sql, psccList);
        } catch (final Exception e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
        }
        
        return null;
    }
    
    private int[] batchInsertPSCC(final String sql, final List<PatientStageCombinationCount> psccList) throws Exception {
        final int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
            
            
            @Override
            public int[] doInTransaction(final TransactionStatus status) {
                final int[] updateCounts = PanaceaPatientDrugComboTasklet.this.jdbcTemplate.batchUpdate(sql,
                    new BatchPreparedStatementSetter() {
                        
                        
                        @Override
                        public void setValues(final PreparedStatement ps, final int i) throws SQLException {
                            ps.setLong(1, psccList.get(i).getPersonId());
                            ps.setString(2, psccList.get(i).getComboIds());
                            ps.setInt(3, psccList.get(i).getStage());
                            ps.setString(4, psccList.get(i).getComboSeq());
                            ps.setDate(5, psccList.get(i).getStartDate());
                            ps.setDate(6, psccList.get(i).getEndDate());
                            ps.setInt(7, psccList.get(i).getDuration());
                            ps.setInt(8, psccList.get(i).getResultVerstion());
                            ps.setInt(9, psccList.get(i).getGapDays());
                        }
                        
                        @Override
                        public int getBatchSize() {
                            return psccList.size();
                        }
                    });
                    
                return updateCounts;
            }
        });
        return ret;
    }
    
    private void loadStudyPncStgCombo(final Long studyId, final Map<String, Object> jobParams) {
        //        final List<PanaceaStageCombination> pncStgCmbList = this.pncStageCombinationRepository
        //                .getAllStageCombination(this.pncStudy.getStudyId());
        //      final List<PanaceaStageCombination> pncStgCmbList = PanaceaUtil.loadStudyStageCombination(studyId, template, resultsTableQualifier, sourceDialect)
        //      .getAllStageCombination(this.pncStudy.getStudyId());
        final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        final List<PanaceaStageCombination> pncStgCmbList = PanaceaUtil.loadStudyStageCombination(studyId, this.jdbcTemplate,
            resultsTableQualifier, sourceDialect);
        
        this.pncStgComboMap.clear();
        
        for (final PanaceaStageCombination pncStgCombo : pncStgCmbList) {
            if ((pncStgCombo != null) && (pncStgCombo.getCombMapList() != null) && (pncStgCombo.getCombMapList() != null)) {
                final String key = this.generateComboKey(this.pncStgSingleConceptComboMap, pncStgCombo);
                if (!StringUtils.isEmpty(key)) {
                    this.pncStgComboMap.put(key, pncStgCombo);
                }
            }
        }
    }
    
    private String generateComboKey(final Map<String, Long> singleConceptComboMap,
                                    final PanaceaStageCombination pncStgCombo) {
        if ((pncStgCombo != null) && (singleConceptComboMap != null) && (pncStgCombo.getCombMapList() != null)) {
            if ((pncStgCombo.getCombMapList().size() == 1)
                    && singleConceptComboMap.containsKey(pncStgCombo.getCombMapList().get(0).getConceptId().toString())) {
                return pncStgCombo.getPncTxStgCmbId().toString();
            } else if (pncStgCombo.getCombMapList().size() == 1) {
                // TODO -- error logging
                return null;
            } else {
                String keyString = "";
                for (final PanaceaStageCombinationMap comboMap : pncStgCombo.getCombMapList()) {
                    final Long comboConceptComboId = singleConceptComboMap.get(comboMap.getConceptId().toString());
                    
                    if (comboConceptComboId != null) {
                        keyString += StringUtils.isEmpty(keyString) ? comboConceptComboId.toString()
                                : "|" + comboConceptComboId.toString();
                    } else {
                        // TODO -- error logging
                    }
                }
                
                return keyString;
            }
        } else {
            // TODO -- error logging
            return null;
        }
    }
    
    private void loadStudyPncStgSingleConceptCombo(final Map<String, Object> jobParams) {
        // script order by combo_id
        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/getSingleConceptCombo.sql");
        
        final String resultsTableQualifier = (String) jobParams.get("results_schema");
        final String sourceDialect = (String) jobParams.get("sourceDialect");
        
        final String[] params = new String[] { "results_schema", "studyId" };
        final String[] values = new String[] { resultsTableQualifier, this.pncStudy.getStudyId().toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        log.debug("PanaceaPatientDrugComboTasklet.loadStudyPncStgSingleConceptCombo, begin... ");
        
        final List<PanaceaStageCombination> singleConceptPncStgCombo = this.jdbcTemplate.query(sql,
            new RowMapper<PanaceaStageCombination>() {
                
                
                @Override
                public PanaceaStageCombination mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                    final PanaceaStageCombination pncStgCombo = new PanaceaStageCombination();
                    pncStgCombo.setPncTxStgCmbId(rs.getLong("combo_id"));
                    pncStgCombo.setStudyId(PanaceaPatientDrugComboTasklet.this.pncStudy.getStudyId());
                    
                    final List<PanaceaStageCombinationMap> mapList = new ArrayList<PanaceaStageCombinationMap>();
                    final PanaceaStageCombinationMap pncMap = new PanaceaStageCombinationMap();
                    pncMap.setConceptId(rs.getLong("concept_id"));
                    mapList.add(pncMap);
                    
                    pncStgCombo.setCombMapList(mapList);
                    
                    return pncStgCombo;
                }
            });
        
        log.debug("PanaceaPatientDrugComboTasklet.loadStudyPncStgSingleConceptCombo, returned size -- "
                + singleConceptPncStgCombo.size());
        
        this.pncStgSingleConceptComboMap.clear();
        
        for (final PanaceaStageCombination combo : singleConceptPncStgCombo) {
            if (!this.pncStgSingleConceptComboMap.containsKey(combo.getCombMapList().get(0).getConceptId().toString())) {
                
                this.pncStgSingleConceptComboMap.put(combo.getCombMapList().get(0).getConceptId().toString(),
                    combo.getPncTxStgCmbId());
            }
        }
    }
    
    /**
     * OHDSI-75: For all 3 databases now, difference is sql server version cannot use sequence for
     * Janssen... Originally: sql server workaround (hibernate does not support sequence for sql
     * server dialect. Cause id always 0 for saved/persistent objects. That cases same session
     * multiple objects with the same id exceptions
     */
    private void saveComboMap(final List<PanaceaStageCombination> newPSCombo, final Map<String, Object> jobParams,
                              final Long studyId) {
        if ((newPSCombo != null) && (newPSCombo.size() > 0)) {
            String sql = "";
            
            final String sourceDialect = (String) jobParams.get("sourceDialect");
            
            if ("sql server".equalsIgnoreCase(sourceDialect)) {
                for (final PanaceaStageCombination psc : newPSCombo) {
                    sql += "delete from #_pnc_tmp_mssql_seq_id; \n "
                            + "insert INTO @results_schema.pnc_tx_stage_combination (STUDY_ID) \n" + "values (@studyId) \n"
                            //+ "GO \n"
                            + "SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY]; \n"
                            //+ "GO \n"
                            + "insert into #_pnc_tmp_mssql_seq_id (nextcombid) \n" + "SELECT @@IDENTITY AS [@@IDENTITY]; \n";
                    //+ "GO \n";
                    
                    for (final PanaceaStageCombinationMap pscm : psc.getCombMapList()) {
                        sql += "insert into @results_schema.pnc_tx_stage_combination_map (pnc_tx_stg_cmb_id, concept_id, concept_name) \n"
                                + "select nextcombid, \n" + pscm.getConceptId().toString() + ", '" + pscm.getConceptName()
                                + "' from #_pnc_tmp_mssql_seq_id; \n";
                    }
                }
            } else if ("postgresql".equalsIgnoreCase(sourceDialect)) {
                for (final PanaceaStageCombination psc : newPSCombo) {
                    sql += "delete from #_pnc_tmp_mssql_seq_id; \n "
                            + "insert into #_pnc_tmp_mssql_seq_id select nextval('@results_schema.seq_pnc_tx_stg_cmb'); \n";
                    
                    for (final PanaceaStageCombinationMap pscm : psc.getCombMapList()) {
                        sql += "insert into @results_schema.pnc_tx_stage_combination_map (pnc_tx_stg_cmb_mp_id, pnc_tx_stg_cmb_id, concept_id, concept_name) \n"
                                + "select nextval('@results_schema.seq_pnc_tx_stg_cmb_mp'), nextcombid, \n"
                                + pscm.getConceptId().toString() + ", '" + pscm.getConceptName()
                                + "' from #_pnc_tmp_mssql_seq_id; \n";
                    }
                    
                    sql += "insert INTO @results_schema.pnc_tx_stage_combination (PNC_TX_STG_CMB_ID,STUDY_ID) \n"
                            + "select nextcombid, @studyId from #_pnc_tmp_mssql_seq_id; \n";
                }
            } else {
                //oracle as default
                for (final PanaceaStageCombination psc : newPSCombo) {
                    sql += "delete from #_pnc_tmp_mssql_seq_id; \n "
                            + "insert into #_pnc_tmp_mssql_seq_id select @results_schema.seq_pnc_tx_stg_cmb.NEXTVAL from dual; \n";
                    
                    for (final PanaceaStageCombinationMap pscm : psc.getCombMapList()) {
                        sql += "insert into @results_schema.pnc_tx_stage_combination_map (pnc_tx_stg_cmb_mp_id, pnc_tx_stg_cmb_id, concept_id, concept_name) \n"
                                + "select @results_schema.seq_pnc_tx_stg_cmb_mp.NEXTVAL, nextcombid, \n"
                                + pscm.getConceptId().toString() + ", '" + pscm.getConceptName()
                                + "' from #_pnc_tmp_mssql_seq_id; \n";
                    }
                    
                    sql += "insert INTO @results_schema.pnc_tx_stage_combination (PNC_TX_STG_CMB_ID,STUDY_ID) \n"
                            + "select nextcombid, @studyId from #_pnc_tmp_mssql_seq_id; \n";
                }
            }
            
            sql += "delete from #_pnc_tmp_mssql_seq_id; \n";
            
            final String cdmTableQualifier = (String) jobParams.get("cdm_schema");
            final String resultsTableQualifier = (String) jobParams.get("ohdsi_schema");
            
            final String[] params = new String[] { "cdm_schema", "results_schema", "studyId" };
            final String[] values = new String[] { cdmTableQualifier, resultsTableQualifier, studyId.toString() };
            
            sql = SqlRender.renderSql(sql, params, values);
            sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
            
            this.batchUpdate(sql);
        }
    };
    
    /**
     * OHDSI-75: For all 3 databases now, difference is sql server version cannot use sequence for
     * Janssen... Originally: sql server workaround (hibernate does not support sequence for sql
     * server dialect. Cause id always 0 for saved/persistent objects. That cases same session
     * multiple objects with the same id exceptions
     */
    private void batchUpdate(final String sql) {
        
        final int[] ret = this.transactionTemplate.execute(new TransactionCallback<int[]>() {
            
            
            @Override
            public int[] doInTransaction(final TransactionStatus status) {
                final String[] stmts = SqlSplit.splitSql(sql);
                
                int[] updateCounts = null;
                //ahh - workaround the exceptions thrown for sql server, spring throw "statement returns a ResultSet" exceptions.
                Object returnObj = null;
                
                try {
                    returnObj = PanaceaPatientDrugComboTasklet.this.jdbcTemplate.batchUpdate(stmts);
                } catch (Exception e) {
                    log.error("PanaceaPatientDrugComboTasklet.batchUpdate: " + e);
                }
                
                if (returnObj instanceof int[]) {
                    updateCounts = (int[]) returnObj;
                }
                
                return updateCounts;
            }
        });
        
        if (ret != null) {
            log.debug("Finished adding combo/map in batchUpdate: " + ret.toString());
        }
    }
}
