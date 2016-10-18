/**
 * The contents of this file are subject to the Regenstrief Public License
 * Version 1.0 (the "License"); you may not use this file except in compliance
 * with the License. Please contact Regenstrief Institute if you would like to
 * obtain a copy of the license.
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 * the specific language governing rights and limitations under the License.
 *
 * Copyright (C) Regenstrief Institute. All Rights Reserved.
 */
package org.ohdsi.webapi.panacea.repository.impl;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.EntityManager;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.TerminateJobStepExceptionHandler;
import org.ohdsi.webapi.helper.ResourceHelper;
import org.ohdsi.webapi.job.JobExecutionResource;
import org.ohdsi.webapi.job.JobTemplate;
import org.ohdsi.webapi.panacea.mapper.PanaceaStageCombinationMapper;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummary;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummaryLight;
import org.ohdsi.webapi.panacea.repository.PanaceaStudyRepository;
import org.ohdsi.webapi.service.AbstractDaoService;
import org.ohdsi.webapi.service.SourceService;
import org.ohdsi.webapi.service.VocabularyService;
import org.ohdsi.webapi.source.Source;
import org.ohdsi.webapi.source.SourceDaimon;
import org.ohdsi.webapi.source.SourceInfo;
import org.ohdsi.webapi.vocabulary.Concept;
import org.ohdsi.webapi.vocabulary.ConceptSetExpression;
import org.ohdsi.webapi.vocabulary.ConceptSetExpression.ConceptSetItem;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 */
@Path("/panacea")
@Component
public class PanaceaService extends AbstractDaoService {

    private static final Log log = LogFactory.getLog(PanaceaService.class);

    @Autowired
    private PanaceaStudyRepository panaceaStudyRepository;

    //@Autowired
    //private PanaceaStageCombinationRepository pncStageCombinationRepository;
    //@Autowired
    //private PanaceaStageCombinationMapRepository pncStageCombinationMapRepository;
    //@Autowired
    //private PanaceaPatientSequenceCountRepository pncPatientSequenceCountRepository;
    @Autowired
    private EntityManager em;

    @Autowired
    private JobTemplate jobTemplate;

    @Autowired
    private JobBuilderFactory jobBuilders;

    @Autowired
    private StepBuilderFactory stepBuilders;

    @Autowired
    private PanaceaJobConfiguration pncJobConfig;

    @Autowired
    private VocabularyService vocabService;

    @Autowired
    private SourceService sourceService;

    /**
     * Get PanaceaStudy by id
     *
     * @param studyId Long
     * @return PanaceaStudy
     */
    @GET
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStudy getPanaceaStudyWithId(@PathParam("id") final Long studyId) {

        final PanaceaStudy study = this.getPanaceaStudyRepository().getPanaceaStudyWithId(studyId);
        return study;
    }

    /**
     * Clone a study and save
     *
     * @param studyId Long
     * @return PanaceaStudy
     */
    @GET
    @Path("/cloneStudy/{studyId}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStudy cloneStudy(@PathParam("studyId") final Long studyId) {
        final PanaceaStudy ps = this.panaceaStudyRepository.getPanaceaStudyWithId(studyId);

        if (ps != null) {
            final PanaceaStudy newStudy = ps.cloneStudy();

            final java.util.Date date = new java.util.Date();

            newStudy.setCreateTime(new Timestamp(date.getTime()));

            return this.saveStudy(newStudy);

        }

        return null;
    }

    /**
     * Get all PanaceaStudy
     *
     * @return PanaceaStudy
     */
    @GET
    @Path("/getAllStudy")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStudy> getAllStudy() {
        final List<PanaceaStudy> studyList = new ArrayList<PanaceaStudy>();

        final Sort studySort = new Sort(Sort.Direction.ASC, "studyName", "createTime");
        final Iterable<PanaceaStudy> allStudy = this.getPanaceaStudyRepository().findAll(studySort);

        if (allStudy != null) {
            for (final PanaceaStudy s : allStudy) {
                if (s != null) {
                    studyList.add(s);
                }
            }
        }

        return studyList;
    }

    /**
     * Get all PanaceaStudy getAllStudyWithLastRunTime
     *
     * @return PanaceaStudy
     */
    @GET
    @Path("/getAllStudyWithLastRunTime")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStudy> getAllStudyWithLastRunTime() {
        final List<PanaceaStudy> psList = this.getAllStudy();

        // in a multi source environment we can not assume that all sources have the required tables
        // if we do and the source is on a different result schema version we throw exceptions when trying
        // to query non-existant tables.
        
        //This is needed for enabling the result icon!!! May be we can swallow the exceptions later...
        if (psList != null) {
            for (final PanaceaStudy ps : psList) {

                final Collection<SourceInfo> sourceCol = this.sourceService.getSources();
                final List<PanaceaSummaryLight> psll = new ArrayList<PanaceaSummaryLight>();

                for (final SourceInfo si : sourceCol) {
                    final Source source = getSourceRepository().findOne(si.sourceId);
                    final JdbcTemplate template = this.getSourceJdbcTemplate(source);
                    final PanaceaSummaryLight psl = PanaceaUtil.getStudySummaryLight(template,
                            source.getTableQualifier(SourceDaimon.DaimonType.Results), getSourceDialect(), ps.getStudyId());
                    if (psl != null) {
                        psll.add(psl);
                    }
                }

                if (psll.size() > 0) {
                    Collections.sort(psll, new Comparator<PanaceaSummaryLight>() {

                        @Override
                        public int compare(final PanaceaSummaryLight o1, final PanaceaSummaryLight o2) {
                            if ((o1.getLastUpdateTime() != null) && (o2.getLastUpdateTime() != null)) {
                                return o1.getLastUpdateTime().compareTo(o2.getLastUpdateTime());
                            }

                            return 0;
                        }

                    });

                    ps.setLastRunTime(psll.get(0).getLastUpdateTime());
                }
            }

        }
        
        
        return psList;
    }

    @GET
    @Path("/getStudySummary/{studyId}/{sourceId}")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaSummary getStudySummary(@PathParam("studyId") final Long studyId,
            @PathParam("sourceId") final Integer sourceId) {
        //OHDSI-75
        //final PanaceaSummary ps = this.panaceaStudyRepository.getPanaceaSummaryByStudyIdSourceId(studyId, sourceId);
        final Source source = getSourceRepository().findOne(sourceId);
        final JdbcTemplate template = this.getSourceJdbcTemplate(source);
        final PanaceaSummary ps = PanaceaUtil.getStudySummary(template,
                source.getTableQualifier(SourceDaimon.DaimonType.Results), getSourceDialect(), studyId);

        if (StringUtils.isEmpty(ps.getStudyResultFiltered())) {
            if (!StringUtils.isEmpty(ps.getStudyResultCollapsed())) {
                ps.setStudyResultUniquePath(PanaceaUtil.mergeFromRootNode(ps.getStudyResultCollapsed()).toString());
                ps.setStudyResultCollapsed(PanaceaUtil.includeNone(ps.getStudyResultCollapsed()));
            } else if (!StringUtils.isEmpty(ps.getStudyResults())) {
                ps.setStudyResultUniquePath(PanaceaUtil.mergeFromRootNode(ps.getStudyResults()).toString());
                ps.setStudyResults(PanaceaUtil.includeNone(ps.getStudyResults()));
            }
        } else {
            ps.setStudyResultUniquePath(PanaceaUtil.mergeFromRootNode(ps.getStudyResultFiltered()).toString());
            ps.setStudyResultFiltered(PanaceaUtil.includeNone(ps.getStudyResultFiltered()));
        }

        PanaceaUtil.setSingleIngredientBeforeAndAfterJSONArray(ps);

        return ps;
    }

    //TODO -- note: heavy load Clob. Be carefule to use: add WS annotation
    //OHDSI-75
    //    public List<PanaceaSummary> getStudySummary(final Long studyId) {
    //        return this.panaceaStudyRepository.getPanaceaSummaryByStudyId(studyId);
    //    }
    //TODO -- note: lazy load Clob.
    //OHDSI-75
    //    public List<PanaceaSummaryLight> getStudySummaryLight(final Long studyId) {
    //        return this.panaceaStudyRepository.getPanaceaSummaryLightByStudyId(studyId);
    //    }
    /**
     * Save a study.
     *
     * @param panaceaStudy PanaceaStudy
     * @return PanaceaStudy
     */
    @POST
    @Path("/savestudy")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PanaceaStudy saveStudy(final PanaceaStudy panaceaStudy) {
        final PanaceaStudy ps = new PanaceaStudy();
        if (panaceaStudy.getStudyId() != null) {
            ps.setStudyId(panaceaStudy.getStudyId());
        }
        ps.setCohortDefId(panaceaStudy.getCohortDefId());
        ps.setConceptSetId(panaceaStudy.getConceptSetId());
        ps.setConcepSetDef(panaceaStudy.getConcepSetDef());
        /**
         * The date is being set to actual date - 1 day (like 3/15/2015 set to
         * 3/14/2015). I think it's because it's set to mid-night. So I am
         * subtract one day here for a quick fix.
         */
        if (panaceaStudy.getEndDate() != null) {
            ps.setEndDate(new Date(panaceaStudy.getEndDate().getTime() + (24 * 60 * 60 * 1000)));
        }
        if (panaceaStudy.getStartDate() != null) {
            ps.setStartDate(new Date(panaceaStudy.getStartDate().getTime() + (24 * 60 * 60 * 1000)));
        }
        ps.setStudyDesc(panaceaStudy.getStudyDesc());
        ps.setStudyDetail(panaceaStudy.getStudyDetail());
        ps.setStudyDuration(panaceaStudy.getStudyDuration());
        ps.setStudyName(panaceaStudy.getStudyName());
        ps.setSwitchWindow(panaceaStudy.getSwitchWindow());
        ps.setMinUnitCounts(panaceaStudy.getMinUnitCounts());
        ps.setMinUnitDays(panaceaStudy.getMinUnitDays());
        ps.setGapThreshold(panaceaStudy.getGapThreshold());
        if (panaceaStudy.getCreateTime() != null) {
            ps.setCreateTime(panaceaStudy.getCreateTime());
        } else {
            final java.util.Date date = new java.util.Date();
            ps.setCreateTime(new Timestamp(date.getTime()));
        }

        final PanaceaStudy newS = this.getPanaceaStudyRepository().save(ps);
        return newS;
    }

    /**
     * Create new PanaceaStudy and save
     *
     * @param newStudy PanaceaStudy
     * @return PanaceaStudy
     */
    @POST
    @Path("/newstudy")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public PanaceaStudy createStudy(final PanaceaStudy newStudy) {
        final PanaceaStudy ps = new PanaceaStudy();
        ps.setCohortDefId(newStudy.getCohortDefId());
        ps.setConcepSetDef(newStudy.getConcepSetDef());
        ps.setEndDate(newStudy.getEndDate());
        ps.setStartDate(newStudy.getStartDate());
        ps.setStudyDesc(newStudy.getStudyDesc());
        ps.setStudyDetail(newStudy.getStudyDetail());
        ps.setStudyDuration(newStudy.getStudyDuration());
        ps.setStudyName(newStudy.getStudyName());
        ps.setSwitchWindow(newStudy.getSwitchWindow());
        final java.util.Date date = new java.util.Date();
        ps.setCreateTime(new Timestamp(date.getTime()));

        return this.getPanaceaStudyRepository().save(ps);
    }

    /**
     * Create new empty PanaceaStudy
     *
     * @return PanaceaStudy
     */
    @GET
    @Path("/getemptynewstudy")
    @Produces(MediaType.APPLICATION_JSON)
    public PanaceaStudy getNewEmptyStudy() {
        final PanaceaStudy ps = new PanaceaStudy();

        return ps;
    }

    /**
     * Get PanaceaStageCombination by id
     *
     * @param pncStageCombId Long
     * @return PanaceaStageCombination
     */
    //    @GET
    //    @Path("/pncstudycombination/{id}")
    //    @Produces(MediaType.APPLICATION_JSON)
    //    public PanaceaStageCombination getPanaceaStageCombinationById(@PathParam("id") final Long pncStageCombId) {
    //        
    //        final PanaceaStageCombination pncStgCmb = this.getPncStageCombinationRepository().getPanaceaStageCombinationById(
    //            pncStageCombId);
    //        
    //        return pncStgCmb;
    //    }
    /**
     * Get PanaceaStageCombination by studyId
     *
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombinationforstudy/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStageCombination> getPanaceaStageCombinationByStudyId(@PathParam("id") final Long studyId) {
        String sql = "select PNC_TX_STG_CMB_ID, STUDY_ID from @panacea_schema.pnc_tx_stage_combination where STUDY_ID = @studyId ";
        sql = SqlRender.renderSql(sql, new String[]{"panacea_schema", "studyId"}, new String[]{getOhdsiSchema(),
            studyId.toString()});
        sql = SqlTranslate.translateSql(sql, getSourceDialect(), getDialect());
        return this.getJdbcTemplate().query(sql, new PanaceaStageCombinationMapper());
    }

    /**
     * Get eagerly fetched PanaceaStageCombination by studyId (with
     * PanaceaStageCombination.combMapList fetched)
     *
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    //    @GET
    //    @Path("/pncstudycombinationwithmapforstudy/{id}")
    //    @Produces(MediaType.APPLICATION_JSON)
    //    public List<PanaceaStageCombination> getPanaceaStageCombinationWithMapByStudyId(@PathParam("id") final Long studyId) {
    //        return this.getPncStageCombinationRepository().getAllStageCombination(studyId);
    //    }
    /**
     * Get PanaceaStageCombinationMap by id
     *
     * @param pncStageCombMpId Long
     * @return PanaceaStageCombinationMap
     */
    //    @GET
    //    @Path("/pncstudycombinationmap/{id}")
    //    @Produces(MediaType.APPLICATION_JSON)
    //    public PanaceaStageCombinationMap getPanaceaStageCombinationMapById(@PathParam("id") final Long pncStageCombMpId) {
    //        
    //        final PanaceaStageCombinationMap pncStgCmbMp = this.getPncStageCombinationMapRepository()
    //                .getPanaceaStageCombinationMapById(pncStageCombMpId);
    //        return pncStgCmbMp;
    //    }
    //    public PanaceaPatientSequenceCount getPanaceaPatientSequenceCountById(final Long ppscId) {
    //        return this.pncPatientSequenceCountRepository.getPanaceaPatientSequenceCountById(ppscId);
    //    }
    /**
     * Get PanaceaStageCombination by studyId
     *
     * @param studyId Long
     * @return List of PanaceaStageCombination
     */
    @GET
    @Path("/pncstudycombinationforstudy/{sourceKey}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PanaceaStageCombination> getPanaceaStageCombinationByStudyId(@PathParam("sourceKey") final String sourceKey,
            @PathParam("id") final Long studyId) {
        /**
         * with sourceKey as a parameter. I don't feel we need the multi-source
         * part for Panacea. Will need to discuss with Jon later.
         */
        final Source source = getSourceRepository().findBySourceKey(sourceKey);
        final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
        String sql = "select PNC_TX_STG_CMB_ID, STUDY_ID from @panacea_schema.pnc_tx_stage_combination where STUDY_ID = @studyId ";
        sql = SqlRender.renderSql(sql, new String[]{"panacea_schema", "studyId"},
                new String[]{tableQualifier, studyId.toString()});
        sql = SqlTranslate.translateSql(sql, source.getSourceDialect(), source.getSourceDialect());
        return this.getSourceJdbcTemplate(source).query(sql, new PanaceaStageCombinationMapper());
    }

    //    /**
    //     * Create a new PanaceaStageCombination and save for a study
    //     * 
    //     * @param studyId Long
    //     * @return PanaceaStageCombination
    //     */
    //    @POST
    //    @Path("/newstudystagecombination")
    //    @Produces(MediaType.APPLICATION_JSON)
    //    @Consumes(MediaType.APPLICATION_JSON)
    //    public PanaceaStageCombination createStudyStageCombination(final Long studyId) {
    //        if (studyId != null) {
    //            final PanaceaStageCombination pncComb = new PanaceaStageCombination();
    //            pncComb.setStudyId(studyId);
    //            
    //            return this.pncStageCombinationRepository.save(pncComb);
    //        } else {
    //            log.error("Study ID is null in PanaceaService.createStudyStageCombination.");
    //            return null;
    //        }
    //    }
    //    public List<PanaceaStageCombination> savePanaceaStageCombinationById(final List<PanaceaStageCombination> pncStageCombinationList) {
    //        
    //        return (List<PanaceaStageCombination>) this.getPncStageCombinationRepository().save(pncStageCombinationList);
    //    }
    public String getPanaceaPatientSequenceCountSql(final Long studyId, final Integer sourceId) {
        final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);

        String sql = ResourceHelper.GetResourceAsString("/resources/panacea/sql/getDrugCohortPatientCount.sql");

        final Source source = getSourceRepository().findOne(sourceId);
        final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
        final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

        final String cohortDefId = pncStudy.getCohortDefId().toString();
        final String[] params = new String[]{"cds_schema", "ohdsi_schema", "cohortDefId", "studyId", "drugConceptId",
            "sourceId"};
        //TODO -- for testing only!!!!!!!!!!!
        final String[] values = new String[]{cdmTableQualifier, resultsTableQualifier, cohortDefId, studyId.toString(),
            "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315",
            (new Integer(source.getSourceId())).toString()};

        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, source.getSourceDialect(), source.getSourceDialect());

        return sql;
    }

    /**
     * Test file to file chunk job
     */
    //    public JobExecutionResource runTestJob() {
    //        
    //        final Job job = this.pncJobConfig.createMarkSheet(this.jobBuilders, this.stepBuilders);
    //        
    //        final JobParametersBuilder builder = new JobParametersBuilder();
    //        final JobParameters jobParameters = builder.toJobParameters();
    //        
    //        final JobExecutionResource jobExec = this.jobTemplate.launch(job, jobParameters);
    //        return jobExec;
    //    }
    /**
     * Test DB to file job
     */
    public void runTestPanaceaJob(final Long studyId, final Integer sourceId) {
        if (studyId != null) {
            final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
            if (pncStudy != null) {
                final Source source = getSourceRepository().findOne(sourceId);
                final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
                final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

                final JobParametersBuilder builder = new JobParametersBuilder();

                final String cohortDefId = pncStudy.getCohortDefId().toString();

                final String sql = this.getPanaceaPatientSequenceCountSql(studyId, sourceId);

                builder.addString("cds_schema", cdmTableQualifier);
                builder.addString("ohdsi_schema", resultsTableQualifier);
                builder.addString("cohortDefId", cohortDefId);
                builder.addString("studyId", studyId.toString());
                //TODO -- for testin only!!!
                builder.addString("drugConceptId", "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315");
                builder.addString("sourceDialect", source.getSourceDialect());

                final JobParameters jobParameters = builder.toJobParameters();

                final Job job = this.pncJobConfig.createPanaceaJob(getSourceJdbcTemplate(source));

                try {

                    /**
                     * Unit test doesn't work with ThreadPoolTaskExecutor. Had
                     * to wait for the job to execute and return. WebAPI works
                     * fine with app server. (In unit test: thread sleeping for
                     * sometime works. it used to be job launched and nothing
                     * happens. no error, no warning. set a break point after
                     * jobTemplate.launch(job, jobParameters) and wait worked.
                     * So sleep works too.)
                     */
                    final JobExecutionResource jobExec = this.jobTemplate.launch(job, jobParameters);
                    //                    try {
                    //                        Thread.sleep(20000);
                    //                    } catch (final InterruptedException ex) {
                    //                        log.error("sleeping thread 222222 goes wrong:");
                    //                        ex.printStackTrace();
                    //                        Thread.currentThread().interrupt();
                    //                    }
                } catch (final ItemStreamException e) {
                    e.printStackTrace();
                }
            }
        } else {//TODO
        }
    }

    /**
     * Test DB to file job
     */
    @GET
    @Path("/testpncjob")
    @Produces(MediaType.APPLICATION_JSON)
    public void testPncJob() {
        runTestPanaceaJob(new Long(18), new Integer(1));
    }

    /**
     * Save and run study
     *
     * @param sourceKey String
     * @param panaceaStudy PancaceaStudy
     * @return Map
     */
    @POST
    @Path("/saveandrunstudy/{sourceKey}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Map saveAndRunStudy(@PathParam("sourceKey") final String sourceKey, final PanaceaStudy panaceaStudy) {
        final PanaceaStudy ps = this.saveStudy(panaceaStudy);

        final JobExecutionResource jer = this.runPncTasklet(sourceKey, ps.getStudyId());

        final Map returnMap = new HashMap();

        returnMap.put("status", jer);
        returnMap.put("savedStudyId", ps.getStudyId());

        return returnMap;
    }

    /**
     * Test DB to file job
     */
    @GET
    @Path("/runPncTasklet/{sourceKey}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public JobExecutionResource runPncTasklet(@PathParam("sourceKey") final String sourceKey,
            @PathParam("id") final Long studyId) {
        /**
         * test: localhost:8080/WebAPI/panacea/runPncTasklet/CCAE/18
         */
        return runPanaceaTasklet(sourceKey, studyId);
    }

    public JobExecutionResource runPanaceaTasklet(final String sourceKey, final Long studyId) {
        if ((studyId != null) && (sourceKey != null)) {
            final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
            if (pncStudy != null) {

                final Source source = getSourceRepository().findBySourceKey(sourceKey);
                if (source != null) {
                    final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
                    final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

                    final JobParametersBuilder builder = new JobParametersBuilder();

                    final String cohortDefId = pncStudy.getCohortDefId().toString();

                    builder.addString("cdm_schema", cdmTableQualifier);
                    builder.addString("ohdsi_schema", resultsTableQualifier);
                    builder.addString("results_schema", resultsTableQualifier);
                    builder.addString("cohortDefId", cohortDefId);
                    builder.addString("studyId", studyId.toString());
                    builder.addString("switchWindow", pncStudy.getSwitchWindow().toString());
                    builder.addDate("STUDY_START_DATE", pncStudy.getStartDate());
                    builder.addDate("STUDY_END_DATE", pncStudy.getEndDate());
                    //TODO -- for testin only!!!
                    //builder.addString("drugConceptId",
                    //   "1301025,1328165,1771162,19058274,918906,923645,933724,1310149,1125315,4304178");

                    //this works locally for resolving the JSON: curl -X POST -H "Content-Type: application/json" -d '{"items" :[{"concept":{"CONCEPT_ID":72714,"CONCEPT_NAME":"Chronic polyarticular juvenile rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"1961000","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true},{"concept":{"CONCEPT_ID":4253901,"CONCEPT_NAME":"Juvenile rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"410795001","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true},{"concept":{"CONCEPT_ID":80809,"CONCEPT_NAME":"Rheumatoid arthritis","STANDARD_CONCEPT":"S","INVALID_REASON":"V","CONCEPT_CODE":"69896004","DOMAIN_ID":"Condition","VOCABULARY_ID":"SNOMED","CONCEPT_CLASS_ID":"Clinical Finding","INVALID_REASON_CAPTION":"Valid","STANDARD_CONCEPT_CAPTION":"Standard"},"isExcluded":false,"includeDescendants":true,"includeMapped":true}]}' http://localhost:8080/WebAPI/CCAE/vocabulary/resolveConceptSetExpression
                    final Map<Long, Concept> cMap = this.resolveConceptExpression(pncStudy.getConcepSetDef());

                    final String drugConceptIdsStr = this.getConceptIdsString(cMap, "drug");
                    final String procedureConceptIdsStr = this.getConceptIdsString(cMap, "procedure");
                    final String allConceptIdsStr = StringUtils.isEmpty(procedureConceptIdsStr) ? drugConceptIdsStr
                            .toString() : drugConceptIdsStr.concat(", " + procedureConceptIdsStr);

                    builder.addString("drugConceptId", drugConceptIdsStr);
                    builder.addString("procedureConceptId", procedureConceptIdsStr);
                    builder.addString("allConceptIdsStr", allConceptIdsStr);

                    builder.addString("sourceDialect", source.getSourceDialect());
                    builder.addString("sourceId", new Integer(source.getSourceId()).toString());

                    if ("sql server".equalsIgnoreCase(source.getSourceDialect())) {
                        builder.addString("rowIdString", "%%physloc%%");
                    } else if ("postgresql".equalsIgnoreCase(source.getSourceDialect())) {
                        builder.addString("rowIdString", "CTID");
                    } else {
                        //Oracle as default (not considering postgres now...)
                        builder.addString("rowIdString", "rowid");
                    }

                    //move this out of param and stick into tasklet instead
//                    String drugEraStudyOptionalDateConstraint = "";
//                    if (pncStudy.getStartDate() != null) {
//                        if("sql server".equalsIgnoreCase(source.getSourceDialect())){
//                            drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
//                                    .concat("AND (era.DRUG_ERA_START_DATE > CONVERT(datetime, '" + pncStudy.getStartDate().toString() + "') OR era.DRUG_ERA_START_DATE = CONVERT(datetime, '" + pncStudy.getStartDate().toString() + "')) \n");
//                        }else{
//                            drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
//                                    .concat("AND (era.DRUG_ERA_START_DATE > to_date('" + pncStudy.getStartDate().toString() + "', 'yyyy-mm-dd') OR era.DRUG_ERA_START_DATE = to_date('" + pncStudy.getStartDate().toString() + "', 'yyyy-mm-dd')) \n");                            
//                        }
//                    }
//                    if (pncStudy.getEndDate() != null) {
//                        if("sql server".equalsIgnoreCase(source.getSourceDialect())){
//                            drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
//                                    .concat("AND (era.DRUG_ERA_START_DATE < CONVERT(datetime, '" + pncStudy.getEndDate().toString() + "') OR era.DRUG_ERA_START_DATE = CONVERT(datetime, '" + pncStudy.getEndDate().toString() + "')) \n");                            
//                        }else{
//                            drugEraStudyOptionalDateConstraint = drugEraStudyOptionalDateConstraint
//                                .concat("AND (era.DRUG_ERA_START_DATE < to_date('" + pncStudy.getEndDate().toString() + "', 'yyyy-mm-dd') OR era.DRUG_ERA_START_DATE = to_date('" + pncStudy.getEndDate().toString() + "', 'yyyy-mm-dd')) \n");
//                        }
//                    }
//
//                    String procedureStudyOptionalDateConstraint = "";
//                    if (pncStudy.getStartDate() != null) {
//                        if("sql server".equalsIgnoreCase(source.getSourceDialect())){
//                            procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
//                                    .concat("AND (proc.PROCEDURE_DATE > CONVERT(datetime, '" + pncStudy.getStartDate().toString() + "') OR proc.PROCEDURE_DATE = CONVERT(datetime, '" + pncStudy.getStartDate().toString() + "')) \n");                            
//                        }else{
//                            procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
//                                .concat("AND (proc.PROCEDURE_DATE > to_date('" + pncStudy.getStartDate().toString() + "', 'yyyy-mm-dd') OR proc.PROCEDURE_DATE = to_date('" + pncStudy.getStartDate().toString() + "', 'yyyy-mm-dd')) \n");
//                        }
//                    }
//                    if (pncStudy.getEndDate() != null) {
//                        if("sql server".equalsIgnoreCase(source.getSourceDialect())){
//                            procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
//                                    .concat("AND (proc.PROCEDURE_DATE < CONVERT(datetime, '" + pncStudy.getEndDate().toString() + "') OR proc.PROCEDURE_DATE = CONVERT(datetime, '" + pncStudy.getEndDate().toString() + "')) \n");                            
//                        }else {
//                            procedureStudyOptionalDateConstraint = procedureStudyOptionalDateConstraint
//                                .concat("AND (proc.PROCEDURE_DATE < to_date('" + pncStudy.getEndDate().toString() + "', 'yyyy-mm-dd') OR proc.PROCEDURE_DATE = to_date('" + pncStudy.getEndDate().toString() + "', 'yyyy-mm-dd')) \n");
//                        }
//                    }
//
//                    builder.addString("drugEraStudyOptionalDateConstraint", drugEraStudyOptionalDateConstraint);
//                    builder.addString("procedureStudyOptionalDateConstraint", procedureStudyOptionalDateConstraint);

                    addTempTableNames(builder, source, resultsTableQualifier);

                    final JobParameters jobParameters = builder.toJobParameters();

                    final PanaceaTasklet pncTasklet = new PanaceaTasklet(this.getSourceJdbcTemplate(source),
                            this.getTransactionTemplate(), this, pncStudy);

                    final Step pncStep1 = this.stepBuilders.get("panaceaStudyStep1").tasklet(pncTasklet)
                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();

                    //                    final PanaceaTasklet2 pncTasklet2 = new PanaceaTasklet2(this.getSourceJdbcTemplate(source),
                    //                            this.getTransactionTemplate(), this, pncStudy);
                    //                    
                    //                    final Step pncStep2 = this.stepBuilders.get("panaceaStudyStep2").tasklet(pncTasklet2)
                    //                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();
                    //                    
                    //                    final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1).next(pncStep2).build();
                    //final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1).build();
                    final PanaceaGetPersonIdsTasklet pncGetPersonIdsTasklet = new PanaceaGetPersonIdsTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate());

                    final Step pncGetPersonIdsTaskletStep = this.stepBuilders.get("pncGetPersonIdsTaskletStep")
                            .tasklet(pncGetPersonIdsTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();

                    //                    final PanaceaPatientDrugComboTasklet pncPatientDrugComboTasklet = new PanaceaPatientDrugComboTasklet(
                    //                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy,
                    //                            this.pncStageCombinationRepository, this.pncStageCombinationMapRepository, this.em);
                    final PanaceaPatientDrugComboTasklet pncPatientDrugComboTasklet = new PanaceaPatientDrugComboTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy, this.em);

                    final Step pncPatientDrugComboTaskletStep = this.stepBuilders.get("pncPatientDrugComboTaskletStep")
                            .tasklet(pncPatientDrugComboTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();

                    final PanaceaSummaryGenerateTasklet pncSummaryTasklet = new PanaceaSummaryGenerateTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy);

                    final Step pncSummaryStep = this.stepBuilders.get("pncSummaryStep").tasklet(pncSummaryTasklet)
                            .exceptionHandler(new TerminateJobStepExceptionHandler()).build();

                    final PanaceaFiilteredSummaryGenerateTasklet pncFilteredSummaryTasklet = new PanaceaFiilteredSummaryGenerateTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy);

                    final Step pncFilteredSummaryStep = this.stepBuilders.get("pncFilteredSummaryStep")
                            .tasklet(pncFilteredSummaryTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();

                    final Job pncStudyJob = this.jobBuilders.get("panaceaStudy").start(pncStep1)
                            .next(pncGetPersonIdsTaskletStep).next(pncPatientDrugComboTaskletStep).next(pncSummaryStep)
                            .next(pncFilteredSummaryStep).build();

                    final JobExecutionResource jobExec = this.jobTemplate.launch(pncStudyJob, jobParameters);

                    return jobExec;
                } else {
                    //TODO
                    log.error("");
                }
            }
            return null;
        } else {
            //TODO
            log.error("");
            return null;
        }
    }

    @GET
    @Path("/runPncFilterSummaryTasklet/{sourceKey}/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public JobExecutionResource runPncFilterSummaryTasklet(@PathParam("sourceKey") final String sourceKey,
            @PathParam("id") final Long studyId) {
        /**
         * test:
         * localhost:8080/WebAPI/panacea/runPncFilterSummaryTasklet/RIV5/19
         */
        return runPanaceaFilterSummaryTasklet(sourceKey, studyId);
    }

    public JobExecutionResource runPanaceaFilterSummaryTasklet(final String sourceKey, final Long studyId) {
        if ((studyId != null) && (sourceKey != null)) {
            final PanaceaStudy pncStudy = this.getPanaceaStudyWithId(studyId);
            if (pncStudy != null) {

                final Source source = getSourceRepository().findBySourceKey(sourceKey);
                if (source != null) {
                    final String resultsTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);
                    final String cdmTableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.CDM);

                    final JobParametersBuilder builder = new JobParametersBuilder();

                    final String cohortDefId = pncStudy.getCohortDefId().toString();

                    builder.addString("cdm_schema", cdmTableQualifier);
                    builder.addString("ohdsi_schema", resultsTableQualifier);
                    builder.addString("results_schema", resultsTableQualifier);
                    builder.addString("studyId", studyId.toString());
                    builder.addString("sourceDialect", source.getSourceDialect());
                    builder.addString("sourceId", new Integer(source.getSourceId()).toString());

                    addTempTableNames(builder, source, resultsTableQualifier);

                    if ("sql server".equalsIgnoreCase(source.getSourceDialect())) {
                        builder.addString("rowIdString", "%%physloc%%");
                    } else if ("postgresql".equalsIgnoreCase(source.getSourceDialect())) {
                        builder.addString("rowIdString", "CTID");
                    } else {
                        //Oracle as default (not considering postgres now...)
                        builder.addString("rowIdString", "rowid");
                    }

                    final JobParameters jobParameters = builder.toJobParameters();

                    final PanaceaFiilteredSummaryGenerateTasklet pncFilteredSummaryTasklet = new PanaceaFiilteredSummaryGenerateTasklet(
                            this.getSourceJdbcTemplate(source), this.getTransactionTemplate(), pncStudy);

                    final Step pncFilteredSummaryStep = this.stepBuilders.get("pncFilteredSummaryStep")
                            .tasklet(pncFilteredSummaryTasklet).exceptionHandler(new TerminateJobStepExceptionHandler())
                            .build();

                    final Job pncStudyJob = this.jobBuilders.get("panaceaFilterSummaryStudy").start(pncFilteredSummaryStep)
                            .build();

                    final JobExecutionResource jobExec = this.jobTemplate.launch(pncStudyJob, jobParameters);

                    return jobExec;
                } else {
                    //TODO
                    log.error("");
                    return null;
                }
            }
            return null;
        } else {
            //TODO
            log.error("");
            return null;
        }
    }

    //TODO - find a common/util place for this later...
    private void addTempTableNames(final JobParametersBuilder builder, final Source source,
            final String resultsTableQualifier) {
        if ("sql server".equalsIgnoreCase(source.getSourceDialect()) || "postgresql".equalsIgnoreCase(source.getSourceDialect())) {
            builder.addString("pnc_ptsq_ct", resultsTableQualifier + ".pnc_tmp_ptsq_ct");
            builder.addString("pnc_ptstg_ct", resultsTableQualifier + ".pnc_tmp_ptstg_ct");
            builder.addString("pnc_tmp_cmb_sq_ct", resultsTableQualifier + ".pnc_tmp_cmb_sq_ct");

            builder.addString("pnc_smry_msql_cmb", resultsTableQualifier + ".pnc_tmp_smry_msql_cmb");
            builder.addString("pnc_indv_jsn", resultsTableQualifier + ".pnc_tmp_indv_jsn");
            builder.addString("pnc_unq_trtmt", resultsTableQualifier + ".pnc_tmp_unq_trtmt");
            builder.addString("pnc_unq_pth_id", resultsTableQualifier + ".pnc_tmp_unq_pth_id");

            builder.addString("pnc_smrypth_fltr", resultsTableQualifier + ".pnc_tmp_smrypth_fltr");
            builder.addString("pnc_smry_ancstr", resultsTableQualifier + ".pnc_tmp_smry_ancstr");
        } else {
            //Oracle as default (not considering postgres now...)
            builder.addString("pnc_ptsq_ct", "#_pnc_ptsq_ct");
            builder.addString("pnc_ptstg_ct", "#_pnc_ptstg_ct");
            builder.addString("pnc_tmp_cmb_sq_ct", "#_pnc_tmp_cmb_sq_ct");

            builder.addString("pnc_smry_msql_cmb", "#_pnc_smry_msql_cmb");
            builder.addString("pnc_indv_jsn", "#_pnc_indv_jsn");
            builder.addString("pnc_unq_trtmt", "#_pnc_unq_trtmt");
            builder.addString("pnc_unq_pth_id", "#_pnc_unq_pth_id");

            builder.addString("pnc_smrypth_fltr", "#_pnc_smrypth_fltr");
            builder.addString("pnc_smry_ancstr", "#_pnc_smry_ancstr");
        }
    }

    /**
     * @return the panaceaStudyRepository
     */
    public PanaceaStudyRepository getPanaceaStudyRepository() {
        return this.panaceaStudyRepository;
    }

    /**
     * @param panaceaStudyRepository the panaceaStudyRepository to set
     */
    public void setPanaceaStudyRepository(final PanaceaStudyRepository panaceaStudyRepository) {
        this.panaceaStudyRepository = panaceaStudyRepository;
    }

    /**
     * @return the pncStageCombinationRepository
     */
    //    public PanaceaStageCombinationRepository getPncStageCombinationRepository() {
    //        return this.pncStageCombinationRepository;
    //    }
    /**
     * @param pncStageCombinationRepository the pncStageCombinationRepository to
     * set
     */
    //    public void setPncStageCombinationRepository(final PanaceaStageCombinationRepository pncStageCombinationRepository) {
    //        this.pncStageCombinationRepository = pncStageCombinationRepository;
    //    }
    /**
     * @return the pncStageCombinationMapRepository
     */
    //    public PanaceaStageCombinationMapRepository getPncStageCombinationMapRepository() {
    //        return this.pncStageCombinationMapRepository;
    //    }
    /**
     * @param pncStageCombinationMapRepository the
     * pncStageCombinationMapRepository to set
     */
    //    public void setPncStageCombinationMapRepository(final PanaceaStageCombinationMapRepository pncStageCombinationMapRepository) {
    //        this.pncStageCombinationMapRepository = pncStageCombinationMapRepository;
    //    }
    /**
     * @return the em
     */
    public EntityManager getEm() {
        return this.em;
    }

    /**
     * @param em the em to set
     */
    public void setEm(final EntityManager em) {
        this.em = em;
    }

    /**
     * @return the jobTemplate
     */
    public JobTemplate getJobTemplate() {
        return this.jobTemplate;
    }

    /**
     * @param jobTemplate the jobTemplate to set
     */
    public void setJobTemplate(final JobTemplate jobTemplate) {
        this.jobTemplate = jobTemplate;
    }

    public Map<Long, Concept> resolveConceptExpression(final String expressionString) {
        if (!StringUtils.isEmpty(expressionString)) {
            final ObjectMapper mapper = new ObjectMapper();
            ConceptSetExpression expression;
            try {
                expression = mapper.readValue(expressionString, ConceptSetExpression.class);

                if (expression != null) {
                    final ConceptSetItem[] items = expression.items;
                    if ((items != null) && (items.length > 0)) {
                        final Map<Long, Concept> cMap = new HashMap<Long, Concept>();
                        for (final ConceptSetItem item : items) {
                            if ((item != null) && (item.concept != null)) {
                                cMap.put(item.concept.conceptId, item.concept);
                            }
                        }

                        return cMap;
                    }
                }
            } catch (final JsonParseException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            } catch (final JsonMappingException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            } catch (final IOException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            }
        }

        return null;
    }

    public String getConceptIdsString(final Map<Long, Concept> cMap, final String domainId) {

        if ((cMap != null) && (domainId != null)) {
            String conceptIdsStr = "";

            for (final Entry<Long, Concept> entry : cMap.entrySet()) {
                if ((entry.getKey() != null) && (entry.getValue() != null)) {
                    if (entry.getValue().domainId != null) {
                        if (entry.getValue().domainId.toLowerCase().equals(domainId.toLowerCase())) {
                            conceptIdsStr = StringUtils.isEmpty(conceptIdsStr) ? conceptIdsStr.concat(entry.getKey()
                                    .toString().toString()) : conceptIdsStr.concat("," + entry.getKey().toString());
                        }
                    }
                }
            }

            return conceptIdsStr;
        } else {
            //TODO - error logging...
            return null;
        }
    }

    public List<PanaceaStageCombination> loadStudyStageCombination(final Long studyId, final String sourceKey) {
        final Source source = getSourceRepository().findBySourceKey(sourceKey);
        final String tableQualifier = source.getTableQualifier(SourceDaimon.DaimonType.Results);

        return PanaceaUtil.loadStudyStageCombination(studyId, this.getSourceJdbcTemplate(source), tableQualifier,
                source.getSourceDialect());
    }
}
