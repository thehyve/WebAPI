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
package org.ohdsi.webapi.panacea.pojo;

import java.io.Serializable;
import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 */
//@Entity(name = "PanaceaSummaryLight")
//@Table(name = "pnc_study_summary")
//@IdClass(PanaceaSummaryId.class)
@XmlRootElement(name = "PanaceaSummaryLight")
@XmlAccessorType(XmlAccessType.FIELD)
public class PanaceaSummaryLight implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    //@Id
    //@Column(name = "study_id")
    private Long studyId;
    
    //@Id
    //@Column(name = "source_id")
    private Integer sourceId;
    
    //@Column(name = "last_update_time")
    private Timestamp lastUpdateTime;
    
    //    @Column(name = "STUDY_RESULTS")
    //    private String studyResults;
    //    
    //    @Column(name = "STUDY_RESULTS_2")
    //    private String studyResultCollapsed;
    //    
    //    @Column(name = "STUDY_RESULTS_FILTERED")
    //    private String studyResultFiltered;
    
    /**
     * @return the studyId
     */
    public Long getStudyId() {
        return this.studyId;
    }
    
    /**
     * @param studyId the studyId to set
     */
    public void setStudyId(final Long studyId) {
        this.studyId = studyId;
    }
    
    /**
     * @return the sourceId
     */
    public Integer getSourceId() {
        return this.sourceId;
    }
    
    /**
     * @param sourceId the sourceId to set
     */
    public void setSourceId(final Integer sourceId) {
        this.sourceId = sourceId;
    }
    
    /**
     * @return the lastUpdateTime
     */
    public Timestamp getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    /**
     * @param lastUpdateTime the lastUpdateTime to set
     */
    public void setLastUpdateTime(final Timestamp lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PanaceaSummaryLight [studyId=" + this.studyId + ", sourceId=" + this.sourceId + ", lastUpdateTime="
                + this.lastUpdateTime + "]";
    }
}
