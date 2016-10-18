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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.ohdsi.sql.SqlRender;
import org.ohdsi.sql.SqlTranslate;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombinationMap;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummary;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummaryLight;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummaryLightMapper;
import org.ohdsi.webapi.panacea.pojo.PanaceaSummaryMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

/**
 * A util class mainly for JSON manipulation after calculation (introduced for unique pathway)
 */
public class PanaceaUtil {
    
    private static final Log log = LogFactory.getLog(PanaceaUtil.class);
    
    public static void setSingleIngredientBeforeAndAfterJSONArray(final PanaceaSummary ps) {
        try {
            JSONObject rootNode;
            
            rootNode = new JSONObject(ps.getStudyResultUniquePath());
            
            final JSONArray singleIngredientJSONArray = rootNode.getJSONArray("singleIngredient");
            
            if (!StringUtils.isEmpty(ps.getStudyResultFiltered())) {
                final JSONObject filteredVersionJObj = new JSONObject(ps.getStudyResultFiltered());
                filteredVersionJObj.put("singleIngredient", singleIngredientJSONArray);
                ps.setStudyResultFiltered(filteredVersionJObj.toString());
            }
            if (!StringUtils.isEmpty(ps.getStudyResultCollapsed())) {
                final JSONObject collapseVersionJObj = new JSONObject(ps.getStudyResultCollapsed());
                collapseVersionJObj.put("singleIngredient", singleIngredientJSONArray);
                ps.setStudyResultCollapsed(collapseVersionJObj.toString());
            }
            if (!StringUtils.isEmpty(ps.getStudyResults())) {
                final JSONObject originVersionJObj = new JSONObject(ps.getStudyResults());
                originVersionJObj.put("singleIngredient", singleIngredientJSONArray);
                ps.setStudyResults(originVersionJObj.toString());
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
        }
    }
    
    /**
     * Call mergeNode() for merging for unique pathway
     */
    public static JSONObject mergeFromRootNode(final String summaryJson) {
        try {
            JSONObject rootNode = new JSONObject(summaryJson);
            
            if (rootNode.has("children")) {
                final JSONArray childJsonArray = rootNode.getJSONArray("children");
                
                final JSONArray newChildArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    //final JSONObject merged = mergeObj((JSONObject) childJsonArray.get(i));
                    //                    JSONObject merged = mergeNode((JSONObject) childJsonArray.get(i));
                    //                    merged = mergeSameUniqueDesedentNode(merged);
                    
                    //                    JSONObject merged = mergeSameDesedentNode((JSONObject) childJsonArray.get(i));
                    if (((JSONObject) childJsonArray.get(i)).has("uniqueConceptsArray")) {
                        final JSONObject merged = PanaceaUtil.mergeNode((JSONObject) childJsonArray.get(i));
                        
                        try {
                            final Map<Integer, String> oneConcept = getAddedOneConceptId(merged);
                            
                            final Map.Entry<Integer, String> entry = oneConcept.entrySet().iterator().next();
                            
                            merged.put("simpleUniqueConceptId", entry.getKey().intValue());
                            merged.put("simpleUniqueConceptName", entry.getValue());
                            merged.put("simpleUniqueConceptPercentage", merged.get("percentage"));
                            
                        } catch (final JSONException e) {
                            // TODO Auto-generated catch block
                            log.error("Error generated", e);
                            e.printStackTrace();
                        }
                        
                        newChildArray.put(merged);
                    }
                }
                
                rootNode.remove("children");
                if (newChildArray.length() > 0) {
                    rootNode.putOpt("children", newChildArray);
                }
                
                rootNode = mergeSameRingSameParentDuplicates(rootNode);
                
                final JSONArray beforeAndAfterIngredientJSONArray = getSingleIngredientBeforeAndAfter(rootNode);
                
                rootNode.put("singleIngredient", beforeAndAfterIngredientJSONArray);
            }
            
            return rootNode;
        } catch (final Exception e) {
            e.printStackTrace();
        }
        
        return null;
    }
    
    public static Set<Integer> getUniqueConceptIds(final JSONObject jsonObj) {
        if (jsonObj != null) {
            final Set<Integer> conceptIds = new HashSet<Integer>();
            
            JSONArray uniqueConceptArray;
            try {
                uniqueConceptArray = jsonObj.getJSONArray("uniqueConceptsArray");
                
                if (uniqueConceptArray != null) {
                    for (int i = 0; i < uniqueConceptArray.length(); i++) {
                        if (uniqueConceptArray.get(i) != null) {
                            final Integer conceptId = new Integer(
                                    ((JSONObject) uniqueConceptArray.get(i)).getInt("innerConceptId"));
                            
                            conceptIds.add(conceptId);
                        }
                    }
                }
                
                return conceptIds;
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    public static Map<Integer, String> getUniqueConceptIdsMap(final JSONObject jsonObj) {
        if (jsonObj != null) {
            final Map<Integer, String> conceptIds = new HashMap<Integer, String>();
            
            JSONArray uniqueConceptArray;
            try {
                uniqueConceptArray = jsonObj.getJSONArray("uniqueConceptsArray");
                
                if (uniqueConceptArray != null) {
                    for (int i = 0; i < uniqueConceptArray.length(); i++) {
                        if (uniqueConceptArray.get(i) != null) {
                            final Integer conceptId = new Integer(
                                    ((JSONObject) uniqueConceptArray.get(i)).getInt("innerConceptId"));
                            final String conceptName = ((JSONObject) uniqueConceptArray.get(i))
                                    .getString("innerConceptName");
                            
                            conceptIds.put(conceptId, conceptName);
                        }
                    }
                }
                
                return conceptIds;
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    public static Map<Integer, String> getAddedOneConceptId(final JSONObject parent, final JSONObject child) {
        if ((parent != null) && (child != null)) {
            final Map<Integer, String> parentUniqueIds = getUniqueConceptIdsMap(parent);
            final Map<Integer, String> childUniqueIds = getUniqueConceptIdsMap(child);
            
            if ((parentUniqueIds != null) && (childUniqueIds != null)) {
                
                for (final Map.Entry<Integer, String> entry : parentUniqueIds.entrySet()) {
                    childUniqueIds.remove(entry.getKey());
                }
                
                final List<Integer> sortChildIdsList = new ArrayList<Integer>(childUniqueIds.keySet());
                Collections.sort(sortChildIdsList);
                
                if ((sortChildIdsList != null) && (sortChildIdsList.size() > 0)) {
                    final Map<Integer, String> returnIdMap = new HashMap<Integer, String>();
                    returnIdMap.put(sortChildIdsList.get(0), childUniqueIds.get(sortChildIdsList.get(0)));
                    
                    return returnIdMap;
                }
            }
        }
        return null;
    }
    
    public static Map<Integer, String> getAddedOneConceptId(final JSONObject parent) {
        if (parent != null) {
            final Map<Integer, String> parentUniqueIds = getUniqueConceptIdsMap(parent);
            final List<Integer> sortChildIdsList = new ArrayList<Integer>(parentUniqueIds.keySet());
            Collections.sort(sortChildIdsList);
            
            if ((sortChildIdsList != null) && (sortChildIdsList.size() > 0)) {
                final Map<Integer, String> returnIdMap = new HashMap<Integer, String>();
                returnIdMap.put(sortChildIdsList.get(0), parentUniqueIds.get(sortChildIdsList.get(0)));
                
                return returnIdMap;
            }
        }
        
        return null;
    }
    
    public static JSONObject mergeRemoveAction(final JSONObject parent, final JSONObject child) {
        if (getUniqueConceptIds(parent).equals(getUniqueConceptIds(child))) {
            return null;
        } else {
            //This is for adding Jon's one "simple unique array" - one drug only path
            final Map<Integer, String> addedOneSimpleId = getAddedOneConceptId(parent, child);
            try {
                final Map.Entry<Integer, String> entry = addedOneSimpleId.entrySet().iterator().next();
                
                child.put("simpleUniqueConceptId", entry.getKey().intValue());
                child.put("simpleUniqueConceptName", entry.getValue());
                
                final double percentage = (((double) child.getInt("patientCount")) / ((double) parent.getInt("patientCount"))) * (100);
                
                final double rounded = (double) Math.round(percentage * 100) / 100;
                
                child.put("simpleUniqueConceptPercentage", rounded);
                
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
            
            return child;
        }
    }
    
    /**
     * For merging adjacent descendant units according to the same unique treatments.
     * 
     * @param node JSONObject
     * @return JSONObject
     */
    public static JSONObject mergeNode(final JSONObject node) {
        
        try {
            if (node.has("children") && (node.getJSONArray("children") != null)
                    && (node.getJSONArray("children").length() > 0)) {
                
                //non leaf node
                
                final JSONArray childJsonArray = node.getJSONArray("children");
                
                final JSONArray remainedChildJsonArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    //JSONObject grandChild = mergeNode(childJsonArray.getJSONObject(i));
                    mergeNode(childJsonArray.getJSONObject(i));
                    
                    final JSONObject newChild = mergeRemoveAction(node, childJsonArray.getJSONObject(i));
                    
                    if (newChild == null) {
                        //TODO -- calculate other parameters
                    } else {
                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    }
                }
                
                node.remove("children");
                if (remainedChildJsonArray.length() > 0) {
                    node.putOpt("children", remainedChildJsonArray);
                }
                
                return node;
                
            } else {
                //leaf
                return node;
            }
            
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return node;
    }
    
    //Call this after merging adjacent duplicate units with "simpleUniqueConceptId", "simpleUniqueConceptName" and "simpleUniqueConceptPercentage" added 
    //public static JSONArray mergeSameRingSameParentDuplicates(final JSONArray inputNodes) {
    public static JSONObject mergeSameRingSameParentDuplicates(final JSONObject inputNode) {
        try {
            if (inputNode != null) {
                
                //                for (int i = 0; i < nodes.length(); i++) {
                //final JSONObject node = inputNodes.getJSONObject(i);
                if (inputNode.has("children")) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    
                    final JSONArray mergedChildArray = mergeChildDuplicates(childJsonArray, inputNode);
                    
                    // add the none child
                    if (inputNode.has("patientCount")) {
                        int directChildTotalCount = 0;
                        
                        for (int i = 0; i < childJsonArray.length(); i++) {
                            final JSONObject child = childJsonArray.getJSONObject(i);
                            
                            if ((child != null) && child.has("patientCount")) {
                                directChildTotalCount += child.getInt("patientCount");
                            }
                        }
                        
                        final int currentNodeNoneAfterCount = inputNode.getInt("patientCount") - directChildTotalCount;
                        final JSONObject noneObject = createNoneObject(currentNodeNoneAfterCount);
                        mergedChildArray.put(noneObject);
                        
                    }
                    
                    inputNode.remove("children");
                    if (mergedChildArray != null) {
                        inputNode.put("children", mergedChildArray);
                    }
                }
                
                if (inputNode.has("children") && (inputNode.getJSONArray("children") != null)) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        if (child != null) {
                            mergeSameRingSameParentDuplicates(child);
                        }
                    }
                }
                
                return inputNode;
            } else {
                return null;
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return null;
    }
    
    public static JSONArray getSingleIngredientBeforeAndAfter(final JSONObject inputNode) {
        
        final List<Map<String, Map<String, Map<String, Integer>>>> allNodes = new ArrayList<Map<String, Map<String, Map<String, Integer>>>>();
        calculateSingleIngredientBeforeAndAfter(inputNode, allNodes);
        
        final Map<String, Map<String, Integer>> afterDrugMap = new HashMap<String, Map<String, Integer>>();
        
        for (final Map<String, Map<String, Map<String, Integer>>> nodeMap : allNodes) {
            if (nodeMap != null) {
                final Map.Entry<String, Map<String, Map<String, Integer>>> nodeEntry = nodeMap.entrySet().iterator().next();
                
                final String nodeDrugName = nodeEntry.getKey();
                
                final Map<String, Map<String, Integer>> nodeAncestorDescendantMap = nodeEntry.getValue();
                
                final Map<String, Integer> nodeDescendantMap = nodeAncestorDescendantMap.get("descendant");
                
                for (final Map.Entry<String, Integer> entry : nodeDescendantMap.entrySet()) {
                    if (!afterDrugMap.containsKey(nodeDrugName)) {
                        afterDrugMap.put(nodeDrugName, new HashMap<String, Integer>());
                    }
                    
                    final String descedantDrugName = entry.getKey();
                    final Integer descedantDrugCount = entry.getValue();
                    
                    //TODO -- "currentNodeCount" for the total count
                    if (afterDrugMap.get(nodeDrugName).containsKey(descedantDrugName)) {
                        afterDrugMap.get(nodeDrugName).put(descedantDrugName,
                            afterDrugMap.get(nodeDrugName).get(descedantDrugName) + descedantDrugCount);
                    } else {
                        afterDrugMap.get(nodeDrugName).put(descedantDrugName, descedantDrugCount);
                    }
                }
            }
        }
        
        final Map<String, Map<String, Map<String, Number>>> returnAfterDrugMap = new HashMap<String, Map<String, Map<String, Number>>>();
        
        for (final Map.Entry<String, Map<String, Integer>> entry : afterDrugMap.entrySet()) {
            final String drugName = entry.getKey();
            final Map<String, Integer> descendantMap = entry.getValue();
            
            final Integer totalCount = descendantMap.get("currentNodeCount");
            
            for (final Map.Entry<String, Integer> descendantEntry : descendantMap.entrySet()) {
                if (!returnAfterDrugMap.containsKey(drugName)) {
                    returnAfterDrugMap.put(drugName, new HashMap<String, Map<String, Number>>());
                }
                
                final String descdDrugName = descendantEntry.getKey();
                final Integer descdDrugCount = descendantEntry.getValue();
                
                if (!descendantEntry.getKey().equals("currentNodeCount")) {
                    final Map<String, Number> descdMap = new HashMap<String, Number>();
                    
                    final double percentage = (((double) descdDrugCount) / ((double) totalCount)) * (100);
                    final double rounded = (double) Math.round(percentage * 100) / 100;
                    
                    descdMap.put("count", descdDrugCount);
                    descdMap.put("percentage", rounded);
                    
                    returnAfterDrugMap.get(drugName).put(descdDrugName, descdMap);
                }
            }
            
            final Map<String, Number> descdMap = new HashMap<String, Number>();
            descdMap.put("totalCount", totalCount);
            returnAfterDrugMap.get(drugName).put("totalCount", descdMap);
        }
        
        final List<Map<String, Map<String, Map<String, Integer>>>> allNodesAncestor = new ArrayList<Map<String, Map<String, Map<String, Integer>>>>();
        final Map<String, Integer> parentMap = new HashMap<String, Integer>();
        calculateSingleIngredientBeforeAllNodes(null, inputNode, allNodesAncestor, parentMap);
        
        final Map<String, Map<String, Integer>> beforeDrugMap = new HashMap<String, Map<String, Integer>>();
        
        for (final Map<String, Map<String, Map<String, Integer>>> nodeMap : allNodesAncestor) {
            if (nodeMap != null) {
                final Map.Entry<String, Map<String, Map<String, Integer>>> nodeEntry = nodeMap.entrySet().iterator().next();
                
                final String nodeDrugName = nodeEntry.getKey();
                
                final Map<String, Map<String, Integer>> nodeAncestorDescendantMap = nodeEntry.getValue();
                
                final Map<String, Integer> nodeAncestortMap = nodeAncestorDescendantMap.get("ancestor");
                
                final int nodeCount = nodeAncestortMap.get("currentNodeCount");
                
                for (final Map.Entry<String, Integer> entry : nodeAncestortMap.entrySet()) {
                    if (!beforeDrugMap.containsKey(nodeDrugName)) {
                        beforeDrugMap.put(nodeDrugName, new HashMap<String, Integer>());
                    }
                    
                    final String ancestorDrugName = entry.getKey();
                    
                    if (beforeDrugMap.get(nodeDrugName).containsKey(ancestorDrugName)) {
                        beforeDrugMap.get(nodeDrugName).put(ancestorDrugName,
                            beforeDrugMap.get(nodeDrugName).get(ancestorDrugName) + nodeCount);
                    } else {
                        beforeDrugMap.get(nodeDrugName).put(ancestorDrugName, nodeCount);
                    }
                }
            }
        }
        
        final Map<String, Map<String, Map<String, Number>>> returnBeforeDrugMap = new HashMap<String, Map<String, Map<String, Number>>>();
        
        for (final Map.Entry<String, Map<String, Integer>> entry : beforeDrugMap.entrySet()) {
            final String drugName = entry.getKey();
            final Map<String, Integer> ancestorMap = entry.getValue();
            
            final Integer totalCount = ancestorMap.get("currentNodeCount");
            
            for (final Map.Entry<String, Integer> ancestorEntry : ancestorMap.entrySet()) {
                if (!returnBeforeDrugMap.containsKey(drugName)) {
                    returnBeforeDrugMap.put(drugName, new HashMap<String, Map<String, Number>>());
                }
                
                final String ancestorDrugName = ancestorEntry.getKey();
                final Integer ancestorDrugCount = ancestorEntry.getValue();
                
                if (!ancestorEntry.getKey().equals("currentNodeCount")) {
                    final Map<String, Number> descdMap = new HashMap<String, Number>();
                    
                    final double percentage = (((double) ancestorDrugCount) / ((double) totalCount)) * (100);
                    final double rounded = (double) Math.round(percentage * 100) / 100;
                    
                    descdMap.put("count", ancestorDrugCount);
                    descdMap.put("percentage", rounded);
                    
                    returnBeforeDrugMap.get(drugName).put(ancestorDrugName, descdMap);
                }
            }
            
            final Map<String, Number> ancestorTotalMap = new HashMap<String, Number>();
            ancestorTotalMap.put("totalCount", totalCount);
            returnBeforeDrugMap.get(drugName).put("totalCount", ancestorTotalMap);
        }
        
        final JSONArray returnArray = new JSONArray();
        
        for (final Map.Entry<String, Map<String, Map<String, Number>>> entry : returnAfterDrugMap.entrySet()) {
            final String drugName = entry.getKey();
            
            final Integer totalCount = (Integer) entry.getValue().get("totalCount").get("totalCount");
            
            final Map<String, Map<String, Number>> descendantMap = entry.getValue();
            
            final JSONArray drugDescdArray = new JSONArray();
            
            for (final Map.Entry<String, Map<String, Number>> descendantEntry : descendantMap.entrySet()) {
                final String descdDrugName = descendantEntry.getKey();
                
                if (!descdDrugName.equals("totalCount")) {
                    final Integer descdDrugCount = (Integer) descendantEntry.getValue().get("count");
                    final Double descdDrugPercentage = (Double) descendantEntry.getValue().get("percentage");
                    
                    final JSONObject oneDescdObj = new JSONObject();
                    
                    try {
                        oneDescdObj.put("descendantConceptName", descdDrugName);
                        oneDescdObj.put("descendantCount", descdDrugCount);
                        oneDescdObj.put("descendantPercentage", descdDrugPercentage);
                        
                        drugDescdArray.put(oneDescdObj);
                        
                    } catch (final JSONException e) {
                        // TODO Auto-generated catch block
                        log.error("Error generated", e);
                    }
                }
            }
            
            final JSONArray drugAncestorArray = new JSONArray();
            final Map<String, Map<String, Number>> ancestorMap = returnBeforeDrugMap.get(drugName);
            for (final Map.Entry<String, Map<String, Number>> ancestorEntry : ancestorMap.entrySet()) {
                final String ancestorDrugName = ancestorEntry.getKey();
                
                if (!ancestorDrugName.equals("totalCount")) {
                    final Integer ancestorDrugCount = (Integer) ancestorEntry.getValue().get("count");
                    final Double ancestorDrugPercentage = (Double) ancestorEntry.getValue().get("percentage");
                    
                    final JSONObject onAncestorObj = new JSONObject();
                    
                    try {
                        onAncestorObj.put("ancestorConceptName", ancestorDrugName);
                        onAncestorObj.put("ancestorCount", ancestorDrugCount);
                        onAncestorObj.put("ancestorPercentage", ancestorDrugPercentage);
                        
                        drugAncestorArray.put(onAncestorObj);
                        
                    } catch (final JSONException e) {
                        // TODO Auto-generated catch block
                        log.error("Error generated", e);
                    }
                }
            }
            
            final JSONObject drugObj = new JSONObject();
            try {
                
                drugObj.put("oneDrugName", drugName);
                drugObj.put("totalCount", totalCount);
                drugObj.put("descendantArray", drugDescdArray);
                drugObj.put("ancestorArray", drugAncestorArray);
                
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
            }
            
            returnArray.put(drugObj);
        }
        
        return returnArray;
    }
    
    public static void calculateSingleIngredientBeforeAndAfter(final JSONObject inputNode,
                                                               final List<Map<String, Map<String, Map<String, Integer>>>> allNodes) {
        try {
            if (inputNode != null) {
                
                if (inputNode.has("simpleUniqueConceptName")) {
                    final Map<String, Integer> descendantMap = new HashMap<String, Integer>();
                    
                    if ((!inputNode.has("children")) && inputNode.has("patientCount")) {
                        //leaf
                        //                        if (descendantMap.containsKey("None")) {
                        //                            final int count = descendantMap.get("None") + inputNode.getInt("patientCount");
                        //                            descendantMap.put("None", new Integer(count));
                        //                        } else {
                        //                            descendantMap.put("None", inputNode.getInt("patientCount"));
                        //                        }
                    } else {
                        getNodeAllDescendantsDrug(inputNode, descendantMap);
                    }
                    
                    //calculate current node's "none after" for non-leaf node (leaf node already taken care of in //leaf section above)
                    if ((inputNode.has("children")) && inputNode.has("patientCount")) {
                        int directChildTotalCount = 0;
                        
                        final JSONArray childJsonArray = inputNode.getJSONArray("children");
                        for (int i = 0; i < childJsonArray.length(); i++) {
                            final JSONObject child = childJsonArray.getJSONObject(i);
                            
                            if ((child != null) && child.has("patientCount")) {
                                directChildTotalCount += child.getInt("patientCount");
                            }
                        }
                        
                        final int currentNodeNoneAfterCount = inputNode.getInt("patientCount") - directChildTotalCount;
                        //                        descendantMap.put("None", currentNodeNoneAfterCount);
                    }
                    
                    final Map<String, Map<String, Integer>> ancestorAndDescendantMap = new HashMap<String, Map<String, Integer>>();
                    
                    descendantMap.put("currentNodeCount", inputNode.getInt("patientCount"));
                    ancestorAndDescendantMap.put("descendant", descendantMap);
                    
                    final Map<String, Map<String, Map<String, Integer>>> nodeMap = new HashMap<String, Map<String, Map<String, Integer>>>();
                    
                    nodeMap.put(inputNode.getString("simpleUniqueConceptName"), ancestorAndDescendantMap);
                    
                    allNodes.add(nodeMap);
                } else {
                    //root
                }
                
                if (inputNode.has("children") && (inputNode.getJSONArray("children") != null)) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        
                        calculateSingleIngredientBeforeAndAfter(child, allNodes);
                    }
                }
                
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
    }
    
    public static void getNodeAllDescendantsDrug(final JSONObject inputNode, final Map<String, Integer> descendantMap) {
        try {
            if (inputNode != null) {
                //final Map<String, Integer> descendantMap = new HashMap<String, Integer>();
                if (inputNode.has("children") && inputNode.has("patientCount")) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        
                        if ((child != null) && child.has("simpleUniqueConceptName")) {
                            if (descendantMap.containsKey(child.getString("simpleUniqueConceptName"))) {
                                final int count = descendantMap.get(child.getString("simpleUniqueConceptName"))
                                        + child.getInt("patientCount");
                                descendantMap.put(child.getString("simpleUniqueConceptName"), new Integer(count));
                            } else {
                                descendantMap.put(child.getString("simpleUniqueConceptName"), child.getInt("patientCount"));
                            }
                        }
                    }
                    
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        
                        getNodeAllDescendantsDrug(child, descendantMap);
                    }
                }
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
    }
    
    public static void calculateSingleIngredientBeforeAllNodes(final JSONObject parentNode,
                                                               final JSONObject childNode,
                                                               final List<Map<String, Map<String, Map<String, Integer>>>> allNodes,
                                                               final Map<String, Integer> parentMap) {
        try {
            if (childNode != null) {
                
                //final Map<String, Integer> currentNodeAncestorMap = new HashMap<String, Integer>();
                Map<String, Integer> ancestorMap = new HashMap<String, Integer>();
                
                if (childNode.has("simpleUniqueConceptName")) {
                    ancestorMap = getNodeAllAncestorDrug(parentNode, childNode, parentMap);
                    
                    final Map<String, Map<String, Integer>> ancestorAndDescendantMap = new HashMap<String, Map<String, Integer>>();
                    
                    ancestorMap.put("currentNodeCount", childNode.getInt("patientCount"));
                    ancestorAndDescendantMap.put("ancestor", ancestorMap);
                    
                    final Map<String, Map<String, Map<String, Integer>>> nodeMap = new HashMap<String, Map<String, Map<String, Integer>>>();
                    
                    nodeMap.put(childNode.getString("simpleUniqueConceptName"), ancestorAndDescendantMap);
                    
                    allNodes.add(nodeMap);
                } else {
                    //root
                }
                
                if (childNode.has("children") && (childNode.getJSONArray("children") != null)) {
                    final JSONArray childJsonArray = childNode.getJSONArray("children");
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        
                        calculateSingleIngredientBeforeAllNodes(childNode, child, allNodes, ancestorMap);
                    }
                }
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
    }
    
    public static Map<String, Integer> getNodeAllAncestorDrug(final JSONObject parentNode, final JSONObject childNode,
                                                              final Map<String, Integer> parentNodeAncestorMap) {
        
        try {
            if ((parentNode != null) && (childNode != null)) {
                
                final Map<String, Integer> childAncestorMap = new HashMap<String, Integer>();
                
                childAncestorMap.putAll(parentNodeAncestorMap);
                
                childAncestorMap.put("currentNodeCount", childNode.getInt("patientCount"));
                
                String parentDrugName = "";
                
                if (parentNode.has("simpleUniqueConceptName")) {
                    parentDrugName = parentNode.getString("simpleUniqueConceptName");
                    
                    //remove "None" from non-first ring drugs
                    if (childAncestorMap.containsKey("None")) {
                        childAncestorMap.remove("None");
                    }
                } else {
                    //root as parentNode:
                    parentDrugName = "None";
                }
                
                //no need of this count in fact: current child count is as "currentNodeCount", this is the actually count 
                childAncestorMap.put(parentDrugName, childNode.getInt("patientCount"));
                //if (childAncestorMap.containsKey(parentDrugName)) {
                //childAncestorMap.put(parentDrugName,
                //        childAncestorMap.get(parentDrugName) + childNode.getInt("patientCount"));
                //} else {
                //    childAncestorMap.put(parentDrugName, childNode.getInt("patientCount"));
                //}
                
                return childAncestorMap;
            } else if ((parentNode == null) && (childNode != null)) {
                //root as childNode:
                final Map<String, Integer> childAncestorMap = new HashMap<String, Integer>();
                
                return childAncestorMap;
            } else {
                return null;
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return null;
    }
    
    public static JSONArray mergeChildDuplicates(final JSONArray nodes, final JSONObject parent) {
        try {
            if (nodes != null) {
                final Map<Integer, JSONObject> nodesMap = new HashMap<Integer, JSONObject>();
                
                for (int i = 0; i < nodes.length(); i++) {
                    if (nodes.get(i) != null) {
                        final JSONObject node = nodes.getJSONObject(i);
                        final Integer conceptId = new Integer(node.getInt("simpleUniqueConceptId"));
                        
                        if (nodesMap.containsKey(conceptId)) {
                            final JSONObject existedNode = nodesMap.get(conceptId);
                            
                            final int existedNodePatientCount = existedNode.getInt("patientCount");
                            final int currentNodePatientCount = node.getInt("patientCount");
                            
                            existedNode.put("patientCount", existedNodePatientCount + currentNodePatientCount);
                            
                            int parentCount = 0;
                            if (parent.has("comboId") && parent.getString("comboId").equals("root")) {
                                parentCount = parent.getInt("totalCohortCount");
                            } else {
                                parentCount = parent.getInt("patientCount");
                            }
                            
                            final double percentage = (((double) (existedNodePatientCount + currentNodePatientCount)) / ((double) parentCount)) * (100);
                            final double rounded = (double) Math.round(percentage * 100) / 100;
                            
                            existedNode.put("simpleUniqueConceptPercentage", rounded);
                            
                            if (existedNode.has("children") && node.has("children")) {
                                final JSONArray childJsonArray = existedNode.getJSONArray("children");
                                final JSONArray mergingJsonArray = node.getJSONArray("children");
                                
                                for (int j = 0; j < mergingJsonArray.length(); j++) {
                                    if (mergingJsonArray.get(j) != null) {
                                        childJsonArray.put(mergingJsonArray.get(j));
                                    }
                                }
                                existedNode.put("children", childJsonArray);
                            } else if (node.has("children")) {
                                final JSONArray mergingJsonArray = node.getJSONArray("children");
                                
                                existedNode.put("children", mergingJsonArray);
                            }
                        } else {
                            nodesMap.put(conceptId, node);
                        }
                    }
                }
                
                final JSONArray returnJsonArray = new JSONArray();
                
                for (final Map.Entry<Integer, JSONObject> entry : nodesMap.entrySet()) {
                    returnJsonArray.put(entry.getValue());
                }
                
                return returnJsonArray;
            }
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
        }
        
        return null;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    public static String getJSONObjStringAttr(final JSONObject jsonObj, final String attrName) {
        if (jsonObj != null) {
            try {
                return jsonObj.getString(attrName);
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        
        return null;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    public static int getJSONObjIntegerAttr(final JSONObject jsonObj, final String attrName) {
        if (jsonObj != null) {
            try {
                return jsonObj.getInt(attrName);
            } catch (final JSONException e) {
                // TODO Auto-generated catch block
                log.error("Error generated", e);
                e.printStackTrace();
            }
        }
        return -1;
    }
    
    //NOT being used -- could change for use of merge same comboId with same children...
    //Could consider to overload mergeNode() method for different merging criteria as parameter (unique concepts/current unit's combo) 
    private JSONObject mergeSameDesedentNode(final JSONObject node) {
        
        try {
            if (node.has("children") && (node.getJSONArray("children") != null)
                    && (node.getJSONArray("children").length() > 0)) {
                
                //non leaf node
                
                final JSONArray childJsonArray = node.getJSONArray("children");
                
                final JSONArray remainedChildJsonArray = new JSONArray();
                
                for (int i = 0; i < childJsonArray.length(); i++) {
                    mergeSameDesedentNode(childJsonArray.getJSONObject(i));
                    //mergeSameUniqueDesedentNode(childJsonArray.getJSONObject(i));
                    
                    final JSONObject newChild = mergeRemoveAction(node, childJsonArray.getJSONObject(i));
                    if (newChild == null) {
                        //TODO -- calculate other parameters
                    } else {
                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    }
                    //                    if (getJSONObjIntegerAttr(node, "comboId") == getJSONObjIntegerAttr(childJsonArray.getJSONObject(i),
                    //                        "comboId")) {
                    //                        //TODO -- calculate other parameters
                    //                    } else {
                    //                        remainedChildJsonArray.put(childJsonArray.getJSONObject(i));
                    //                    }
                }
                
                node.remove("children");
                if (remainedChildJsonArray.length() > 0) {
                    node.putOpt("children", remainedChildJsonArray);
                }
                
                return node;
                
            } else {
                //leaf
                return node;
            }
            
        } catch (final JSONException e) {
            // TODO Auto-generated catch block
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return node;
    }
    
    public static JSONObject addNoneToChildren(final JSONObject inputNode) {
        try {
            if (inputNode != null) {
                
                //                for (int i = 0; i < nodes.length(); i++) {
                //final JSONObject node = inputNodes.getJSONObject(i);
                if (inputNode.has("children")) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    
                    // add the none child
                    if (inputNode.has("patientCount")) {
                        int directChildTotalCount = 0;
                        
                        for (int i = 0; i < childJsonArray.length(); i++) {
                            final JSONObject child = childJsonArray.getJSONObject(i);
                            
                            if ((child != null) && child.has("patientCount")) {
                                directChildTotalCount += child.getInt("patientCount");
                            }
                        }
                        
                        final int currentNodeNoneAfterCount = inputNode.getInt("patientCount") - directChildTotalCount;
                        final JSONObject noneObject = createNoneObject(currentNodeNoneAfterCount);
                        childJsonArray.put(noneObject);
                        
                    }
                    
                    inputNode.remove("children");
                    if (childJsonArray != null) {
                        inputNode.put("children", childJsonArray);
                    }
                }
                
                if (inputNode.has("children") && (inputNode.getJSONArray("children") != null)) {
                    final JSONArray childJsonArray = inputNode.getJSONArray("children");
                    for (int i = 0; i < childJsonArray.length(); i++) {
                        final JSONObject child = childJsonArray.getJSONObject(i);
                        if (child != null) {
                            addNoneToChildren(child);
                        }
                    }
                }
                
                return inputNode;
            } else {
                return null;
            }
        } catch (final JSONException e) {
            log.error("Error generated", e);
            e.printStackTrace();
        }
        
        return null;
    }
    
    private static JSONObject createNoneObject(final int noneCount) throws JSONException {
        final JSONObject noneObject = new JSONObject();
        noneObject.put("patientCount", noneCount);
        noneObject.put("conceptName", "None");
        noneObject.put("simpleUniqueConceptName", "None");
        noneObject.put("uniqueConceptsArray", new JSONArray());
        return noneObject;
    }
    
    public static String includeNone(final String res) {
        try {
            final JSONObject root = new JSONObject(res);
            if (root.has("children")) {
                final JSONObject rootWithNone = addNoneToChildren(root);
                if (root != null) {
                    return rootWithNone.toString();
                }
            }
        } catch (final Exception e) {
            log.error("unable to include none", e);
        }
        return res;
    }
    
    public static PanaceaSummary getStudySummary(final JdbcTemplate template, final String resultsTableQualifier,
                                                 final String sourceDialect, final Long studyId) {
        String sql = "select study_id, last_update_time, STUDY_RESULTS, STUDY_RESULTS_2, STUDY_RESULTS_FILTERED \n"
                + "from @results_schema.pnc_study_summary \n" + "where study_id = @studyId";
        
        final String[] params = new String[] { "results_schema", "studyId" };
        final String[] values = new String[] { resultsTableQualifier, studyId.toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        PanaceaSummary ps = null;
        
        if (sql != null) {
            try {
                ps = template.queryForObject(sql, new PanaceaSummaryMapper());
            } catch (final Exception e) {
                log.error("PanaceaUtil.getStudySummary return 0 study summary: " + e);
                //e.printStackTrace();
            }
        }
        
        return ps;
    }
    
    public static PanaceaSummaryLight getStudySummaryLight(final JdbcTemplate template, final String resultsTableQualifier,
                                                           final String sourceDialect, final Long studyId) {
        
        String sql = "select study_id, last_update_time \n" + "from @results_schema.pnc_study_summary \n"
                + "where study_id = @studyId";
        
        final String[] params = new String[] { "results_schema", "studyId" };
        final String[] values = new String[] { resultsTableQualifier, studyId.toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        PanaceaSummaryLight psl = null;
        
        if (sql != null) {
            try {
                psl = template.queryForObject(sql, new PanaceaSummaryLightMapper());
            } catch (final Exception e) {
                log.error("PanaceaUtil.getStudySummaryLight return 0 study summary: " + e);
                //e.printStackTrace();
            }
        }
        
        return psl;
    }
    
    public static List<PanaceaStageCombination> loadStudyStageCombination(final Long studyId, final JdbcTemplate template,
                                                                          final String resultsTableQualifier,
                                                                          final String sourceDialect) {
        String sql = "select comb.pnc_tx_stg_cmb_id as combo_id \n"
                + "from @results_schema.pnc_tx_stage_combination comb \n" + "where comb.study_id = @studyId \n"
                + "order by comb.pnc_tx_stg_cmb_id \n";
        
        final String[] params = new String[] { "results_schema", "studyId" };
        final String[] values = new String[] { resultsTableQualifier, studyId.toString() };
        
        sql = SqlRender.renderSql(sql, params, values);
        sql = SqlTranslate.translateSql(sql, "sql server", sourceDialect, null, resultsTableQualifier);
        
        final List<PanaceaStageCombination> pncStgCombo = template.query(sql, new RowMapper<PanaceaStageCombination>() {
            
            @Override
            public PanaceaStageCombination mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                final PanaceaStageCombination pncStgCombo = new PanaceaStageCombination();
                pncStgCombo.setPncTxStgCmbId(rs.getLong("combo_id"));
                pncStgCombo.setStudyId(studyId);
                
                return pncStgCombo;
            }
        });
        
        if ((pncStgCombo != null) && (pncStgCombo.size() > 0)) {
            String stgComboIds = "";
            final Map<Long, PanaceaStageCombination> comboMap = new HashMap<Long, PanaceaStageCombination>();
            
            for (final PanaceaStageCombination combo : pncStgCombo) {
                stgComboIds += stgComboIds.length() == 0 ? combo.getPncTxStgCmbId().toString() : ", "
                        + combo.getPncTxStgCmbId().toString();
                
                final List<PanaceaStageCombinationMap> mapList = new ArrayList<PanaceaStageCombinationMap>();
                combo.setCombMapList(mapList);
                
                comboMap.put(combo.getPncTxStgCmbId(), combo);
            }
            
            String mapSql = "select combo_map.concept_id as concept_id, combo_map.concept_name concept_name, combo_map.pnc_tx_stg_cmb_id as combo_id, combo_map.pnc_tx_stg_cmb_mp_id as map_id \n"
                    + "from @results_schema.pnc_tx_stage_combination_map combo_map \n"
                    + "where combo_map.pnc_tx_stg_cmb_id in ("
                    + stgComboIds
                    + ") \n"
                    + "order by combo_map.pnc_tx_stg_cmb_id, combo_map.pnc_tx_stg_cmb_mp_id \n";
            
            mapSql = SqlRender.renderSql(mapSql, params, values);
            mapSql = SqlTranslate.translateSql(mapSql, "sql server", sourceDialect, null, resultsTableQualifier);
            
            final List<PanaceaStageCombinationMap> comboMapList = template.query(mapSql,
                new RowMapper<PanaceaStageCombinationMap>() {
                    
                    @Override
                    public PanaceaStageCombinationMap mapRow(final ResultSet rs, final int rowNum) throws SQLException {
                        final PanaceaStageCombinationMap pncStgComboMap = new PanaceaStageCombinationMap();
                        
                        pncStgComboMap.setConceptId(rs.getLong("concept_id"));
                        pncStgComboMap.setConceptName(rs.getString("concept_name"));
                        pncStgComboMap.setPncTxStgCmbId(rs.getLong("combo_id"));
                        pncStgComboMap.setPncTxStgCmbMpId(rs.getLong("map_id"));
                        
                        return pncStgComboMap;
                    }
                });
            
            if ((comboMapList != null) && (comboMapList.size() > 0)) {
                for (final PanaceaStageCombinationMap oneComboMap : comboMapList) {
                    final PanaceaStageCombination combo = comboMap.get(oneComboMap.getPncTxStgCmbId());
                    
                    if ((combo != null) && (combo.getCombMapList() != null)) {
                        oneComboMap.setPncStgCmb(combo);
                        combo.getCombMapList().add(oneComboMap);
                    }
                }
            }
        }
        
        return pncStgCombo;
    }
}
