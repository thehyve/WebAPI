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
package org.ohdsi.webapi.test;

import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohdsi.webapi.WebApi;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummary;
import org.ohdsi.webapi.panacea.repository.impl.PanaceaService;
import org.ohdsi.webapi.service.VocabularyService;
import org.ohdsi.circe.vocabulary.Concept;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Document curl WS testing:
 * 
 * <pre>
 *  curl -X GET -H "Content-Type: application/json" http://localhost:8080/WebAPI/panacea/2
 *  
 *  curl -X POST -H "Content-Type: application/json" -d '{"studyName":"Test adding study","studyDesc":"test as proof of concept","concepSetDef":"{\"items\" :[{\"concept\":{\"CONCEPT_ID\":1301025,\"CONCEPT_NAME\":\"Enoxaparin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"67108\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"17\",\"DESCENDANT_RECORD_COUNT\":\"17\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1328165,\"CONCEPT_NAME\":\"Diltiazem\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"3443\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"473\",\"DESCENDANT_RECORD_COUNT\":\"984\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1771162,\"CONCEPT_NAME\":\"Cefazolin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"2180\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"86\",\"DESCENDANT_RECORD_COUNT\":\"172\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":19058274,\"CONCEPT_NAME\":\"Purified Protein Derivative of Tuberculin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8948\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"0\",\"DESCENDANT_RECORD_COUNT\":\"0\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":918906,\"CONCEPT_NAME\":\"oxybutynin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"32675\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"117\",\"DESCENDANT_RECORD_COUNT\":\"241\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":923645,\"CONCEPT_NAME\":\"Omeprazole\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"7646\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"419\",\"DESCENDANT_RECORD_COUNT\":\"886\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":933724,\"CONCEPT_NAME\":\"Phenazopyridine\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8120\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"28\",\"DESCENDANT_RECORD_COUNT\":\"56\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1310149,\"CONCEPT_NAME\":\"Warfarin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"11289\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"350\",\"DESCENDANT_RECORD_COUNT\":\"757\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1125315,\"CONCEPT_NAME\":\"Acetaminophen\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"161\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"1,258\",\"DESCENDANT_RECORD_COUNT\":\"2,821\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false}]}","cohortDefId":915,"studyDetail":"testing","switchWindow":30,"studyDuration":1095,"startDate":"2009-01-01","endDate":"2013-01-01"}' http://localhost:8080/WebAPI/panacea/newstudy
 *  
 *  curl -X POST -H "Content-Type: application/json" -d '{"studyId":2,"studyName":"Chen test study","studyDesc":"test study","concepSetDef":"{\"items\" :[{\"concept\":{\"CONCEPT_ID\":1301025,\"CONCEPT_NAME\":\"Enoxaparin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"67108\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"17\",\"DESCENDANT_RECORD_COUNT\":\"17\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1328165,\"CONCEPT_NAME\":\"Diltiazem\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"3443\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"473\",\"DESCENDANT_RECORD_COUNT\":\"984\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1771162,\"CONCEPT_NAME\":\"Cefazolin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"2180\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"86\",\"DESCENDANT_RECORD_COUNT\":\"172\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":19058274,\"CONCEPT_NAME\":\"Purified Protein Derivative of Tuberculin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8948\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"0\",\"DESCENDANT_RECORD_COUNT\":\"0\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":918906,\"CONCEPT_NAME\":\"oxybutynin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"32675\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"117\",\"DESCENDANT_RECORD_COUNT\":\"241\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":923645,\"CONCEPT_NAME\":\"Omeprazole\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"7646\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"419\",\"DESCENDANT_RECORD_COUNT\":\"886\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":933724,\"CONCEPT_NAME\":\"Phenazopyridine\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"8120\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"28\",\"DESCENDANT_RECORD_COUNT\":\"56\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1310149,\"CONCEPT_NAME\":\"Warfarin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"11289\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"350\",\"DESCENDANT_RECORD_COUNT\":\"757\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1125315,\"CONCEPT_NAME\":\"Acetaminophen\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"161\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"1,258\",\"DESCENDANT_RECORD_COUNT\":\"2,821\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false}]}","cohortDefId":915,"studyDetail":"testing","switchWindow":30,"studyDuration":1095,"startDate":"2010-02-01","endDate":"2013-01-01"}' http://localhost:8080/WebAPI/panacea/savestudy
 *  
 *  curl -X POST -H "Content-Type: application/json" -d http://localhost:8080/WebAPI/panacea/newstudystagecombination
 * </pre>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = WebApi.class)
@Ignore
public class PanaceaTest extends TestCase {
    
    private static final Log log = LogFactory.getLog(PanaceaTest.class);
    
    @Autowired
    private PanaceaService pncService;
    
    @Autowired
    private VocabularyService vocabService;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    @Ignore
    public void testGetCombination() {
        try {
            /**
             * Use browser test too:
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationforstudy/RIV5/2
             */
            List<PanaceaStageCombination> pncStgCmbs = this.pncService.getPanaceaStageCombinationByStudyId("RIV5", new Long(
                    2));
            log.info("testGetCombination: " + pncStgCmbs.toString());
            
            /**
             * http://localhost:8080/WebAPI/panacea/pncstudycombinationforstudy/2
             */
            pncStgCmbs = this.pncService.getPanaceaStageCombinationByStudyId(new Long(2));
            log.info("testGetCombination without source: " + pncStgCmbs.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    //    @Ignore
    //    public void testLoadCombinationWithMap() {
    //        try {
    //            /**
    //             * Use browser test too: http://localhost:8080/WebAPI/panacea/pncstudycombination/1
    //             */
    //            final PanaceaStageCombination pncStgCmb = this.pncService.getPanaceaStageCombinationById(new Long(2));
    //            log.info("testLoadCombinationWithMap: " + pncStgCmb.toString());
    //            
    //            /**
    //             * http://localhost:8080/WebAPI/panacea/pncstudycombinationmap/2
    //             */
    //            final PanaceaStageCombinationMap map = this.pncService.getPanaceaStageCombinationMapById(new Long(2));
    //            log.info("testLoadCombinationWithMap map: " + map.toString());
    //        } catch (final Exception e) {
    //            e.printStackTrace();
    //        }
    //    }
    //    
    //    @Ignore
    //    public void testGetAllCombForStudyWithMap() {
    //        try {
    //            /**
    //             * http://localhost:8080/WebAPI/panacea/pncstudycombinationwithmapforstudy/2
    //             */
    //            final List<PanaceaStageCombination> combList = this.pncService
    //                    .getPanaceaStageCombinationWithMapByStudyId(new Long(2));
    //            log.info("testGetAllCombForStudyWithMap: " + combList.toString());
    //        } catch (final Exception e) {
    //            e.printStackTrace();
    //        }
    //    }
    //    
    //    @Test
    //    public void testTasklet() {
    //        try {
    //            this.pncService.runPatientSequenceCount(new Long(18));
    //            
    //        } catch (final Exception e) {
    //            e.printStackTrace();
    //        }
    //    }
    //    
    //    @Ignore
    //    public void testGetPanaceaPatientSequenceCountById() {
    //        final PanaceaPatientSequenceCount ppsc = this.pncService.getPanaceaPatientSequenceCountById(new Long(777));
    //        
    //        log.info(ppsc);
    //    }
    //    
    //    @Ignore
    //    public void testInsertCombo() {
    //        final PanaceaStageCombination pncCombo = new PanaceaStageCombination();
    //        pncCombo.setStudyId(new Long(2));
    //        
    //        final PanaceaStageCombinationMap combMap1 = new PanaceaStageCombinationMap();
    //        combMap1.setConceptId(new Long(1125315));
    //        combMap1.setConceptName("Acetaminophen");
    //        
    //        final PanaceaStageCombinationMap combMap2 = new PanaceaStageCombinationMap();
    //        combMap2.setConceptId(new Long(923645));
    //        combMap2.setConceptName("Omeprazole");
    //        
    //        final List<PanaceaStageCombinationMap> mapList = new ArrayList<PanaceaStageCombinationMap>();
    //        mapList.add(combMap1);
    //        mapList.add(combMap2);
    //        
    //        pncCombo.setCombMapList(mapList);
    //        
    //        final List<PanaceaStageCombination> comboArrayList = new ArrayList<PanaceaStageCombination>();
    //        comboArrayList.add(pncCombo);
    //        
    //        final List<PanaceaStageCombination> comboList = this.pncService.savePanaceaStageCombinationById(comboArrayList);
    //        
    //        log.info(comboList);
    //    }
    
    @Ignore
    public void testExpressionResolver() {
        final String expressionString = "{\"items\" :[{\"concept\":{\"CONCEPT_ID\":2003406,\"CONCEPT_NAME\":\"Other and open repair of indirect inguinal hernia with graft or prosthesis\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"53.04\",\"DOMAIN_ID\":\"Procedure\",\"VOCABULARY_ID\":\"ICD9Proc\",\"CONCEPT_CLASS_ID\":\"4-dig billing code\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"0\",\"DESCENDANT_RECORD_COUNT\":\"0\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false},{\"concept\":{\"CONCEPT_ID\":1301025,\"CONCEPT_NAME\":\"Enoxaparin\",\"STANDARD_CONCEPT\":\"S\",\"INVALID_REASON\":\"V\",\"CONCEPT_CODE\":\"67108\",\"DOMAIN_ID\":\"Drug\",\"VOCABULARY_ID\":\"RxNorm\",\"CONCEPT_CLASS_ID\":\"Ingredient\",\"INVALID_REASON_CAPTION\":\"Valid\",\"STANDARD_CONCEPT_CAPTION\":\"Standard\",\"RECORD_COUNT\":\"17\",\"DESCENDANT_RECORD_COUNT\":\"17\"},\"isExcluded\":false,\"includeDescendants\":false,\"includeMapped\":false}]}";
        final Map<Long, Concept> cMap = this.pncService.resolveConceptExpression(expressionString);
        final String drugConceptIdsStr = this.pncService.getConceptIdsString(cMap, "Drug");
        final String procedureConceptIdsStr = this.pncService.getConceptIdsString(cMap, "Procedure");
        
        log.info("testExpressionResolver: " + drugConceptIdsStr);
        log.info("testExpressionResolver: " + procedureConceptIdsStr);
    }
    
    @Ignore
    public void testGetAllStudy() {
        try {
            /**
             * Use browser test too: http://localhost:8080/WebAPI/panacea/getAllStudy
             */
            final List<PanaceaStudy> studyList = this.pncService.getAllStudy();
            
            log.info("testGetAllStudy: " + studyList.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    @Ignore
    public void testGetStudySummary() {
        try {
            //final PanaceaSummary summary = this.pncService.getStudySummary(new Long(31), new Integer(1));
            final PanaceaSummary summary = this.pncService.getStudySummary(new Long(2), new Integer(1));
            
            log.info("testGetStudySummary: " + summary);
            
            final List<PanaceaStudy> psl = this.pncService.getAllStudyWithLastRunTime();
            
            log.info("testGetStudySummary list with last updated time: " + psl);
            
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testLoadComboAndMap() {
        try {
            final List<PanaceaStageCombination> pscl = this.pncService.loadStudyStageCombination(new Long(191), "RIV5");
            
            log.info("testLoadComboAndMap: " + pscl.toString());
            
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
    
    //    @Ignore
    //    public void testGetStudySummaryList() {
    //        try {
    //            final List<PanaceaSummary> summaryList = this.pncService.getStudySummary(new Long(31));
    //            
    //            log.info("testGetStudySummaryList: " + summaryList);
    //        } catch (final Exception e) {
    //            e.printStackTrace();
    //        }
    //    }
    //    
    //    @Ignore
    //    public void testGetStudySummaryLightList() {
    //        try {
    //            final List<PanaceaSummaryLight> summaryList = this.pncService.getStudySummaryLight(new Long(31));
    //            
    //            log.info("testGetStudySummaryLightList: " + summaryList);
    //        } catch (final Exception e) {
    //            e.printStackTrace();
    //        }
    //    }
    
    @Ignore
    public void testGetStudySummaryAndJSON() {
        try {
            //final PanaceaSummary summary = this.pncService.getStudySummary(new Long(31), new Integer(1));
            final PanaceaSummary summary = this.pncService.getStudySummary(new Long(101), new Integer(1));
            
            //            final JSONObject rootNode = new JSONObject(summary.getStudyResultFiltered());
            //            
            //            if (rootNode.has("children")) {
            //                final JSONArray childJsonArray = rootNode.getJSONArray("children");
            //                
            //                final JSONArray newChildArray = new JSONArray();
            //                
            //                for (int i = 0; i < childJsonArray.length(); i++) {
            //                    //final JSONObject merged = mergeObj((JSONObject) childJsonArray.get(i));
            //                    //                    JSONObject merged = mergeNode((JSONObject) childJsonArray.get(i));
            //                    //                    merged = mergeSameUniqueDesedentNode(merged);
            //                    
            //                    //                    JSONObject merged = mergeSameDesedentNode((JSONObject) childJsonArray.get(i));
            //                    final JSONObject merged = PanaceaUtil.mergeNode((JSONObject) childJsonArray.get(i));
            //                    
            //                    newChildArray.put(merged);
            //                }
            //                
            //                rootNode.remove("children");
            //                if (newChildArray.length() > 0) {
            //                    rootNode.putOpt("children", newChildArray);
            //                }
            //            }
            //PanaceaUtil.mergeFromRootNode(rootNode);
            
            log.info("testGetStudySummaryAndJSON: " + summary.getStudyResultUniquePath());
            
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}
