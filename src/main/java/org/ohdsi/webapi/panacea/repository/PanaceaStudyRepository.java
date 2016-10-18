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
package org.ohdsi.webapi.panacea.repository;

import org.ohdsi.webapi.panacea.pojo.PanaceaStudy;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 *
 */
public interface PanaceaStudyRepository extends PagingAndSortingRepository<PanaceaStudy, Long> {
    
    /**
     * @param studyId Long
     * @return PanaceaStudy
     */
    @Query("from PanaceaStudy where study_id = ?")
    public PanaceaStudy getPanaceaStudyWithId(Long studyId);
    
//    @Query("from PanaceaSummary where study_id = ? and source_id = ?")
//    public PanaceaSummary getPanaceaSummaryByStudyIdSourceId(Long studyId, Integer sourceId);
//    
//    @Query("from PanaceaSummary where study_id = ? order by last_update_time")
//    public List<PanaceaSummary> getPanaceaSummaryByStudyId(Long studyId);
//    
//    @Query("from PanaceaSummaryLight where study_id = ? order by last_update_time desc")
//    public List<PanaceaSummaryLight> getPanaceaSummaryLightByStudyId(Long studyId);
}
