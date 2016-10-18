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

import java.io.Serializable;
import java.sql.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 */
@XmlRootElement(name = "PatientStageCombinationCount")
@XmlAccessorType(XmlAccessType.FIELD)
public class PatientStageCombinationCount implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private Long personId;
    
    private String comboIds;
    
    private String comboSeq;
    
    private Integer stage;
    
    private Date startDate;
    
    private Date endDate;
    
    private Integer duration;
    
    /**
     * <pre>
     *      version = 1 => no collapse for non-continuous same drug time periods: 08-JUN-07 ~ 08-JUN-07, 03-NOV-07 ~ 03-NOV-07, consider 2 stages
     *      version = 2 => collapse for non-continuous same drug time periods: 08-JUN-07 ~ 08-JUN-07, 03-NOV-07 ~ 03-NOV-07, consider 1 stage
     * </pre>
     */
    private int resultVerstion = 1;
    
    private int gapDays = 0;
    
    /**
     * @return the personId
     */
    public Long getPersonId() {
        return this.personId;
    }
    
    /**
     * @param personId the personId to set
     */
    public void setPersonId(final Long personId) {
        this.personId = personId;
    }
    
    /**
     * @return the comboIds
     */
    public String getComboIds() {
        return this.comboIds;
    }
    
    /**
     * @param comboIds the comboIds to set
     */
    public void setComboIds(final String comboIds) {
        this.comboIds = comboIds;
    }
    
    /**
     * @return the startDate
     */
    public Date getStartDate() {
        return this.startDate;
    }
    
    /**
     * @param startDate the startDate to set
     */
    public void setStartDate(final Date startDate) {
        if ((this.getEndDate() != null) && (startDate != null)) {
            //TODO -- remove or adding formal validation
            if (startDate.after(this.getEndDate())) {
                System.out.println("error in setting time: setStartDate:" + startDate + "---" + this.getStartDate() + "---"
                        + this.endDate);
            }
        }
        this.startDate = startDate;
    }
    
    /**
     * @return the endDate
     */
    public Date getEndDate() {
        return this.endDate;
    }
    
    /**
     * @param endDate the endDate to set
     */
    public void setEndDate(final Date endDate) {
        if ((this.startDate != null) && (endDate != null)) {
            //TODO -- remove or adding formal validation
            if (this.startDate.after(endDate)) {
                System.out.println("error in setting time: setEndDate:" + this.getStartDate() + "---" + this.getEndDate()
                        + "---" + endDate);
            }
        }
        this.endDate = endDate;
    }
    
    /**
     * @return the stage
     */
    public Integer getStage() {
        return this.stage;
    }
    
    /**
     * @param stage the stage to set
     */
    public void setStage(final Integer stage) {
        this.stage = stage;
    }
    
    /**
     * @return the duration
     */
    public Integer getDuration() {
        return this.duration;
    }
    
    /**
     * @param duration the duration to set
     */
    public void setDuration(final Integer duration) {
        this.duration = duration;
    }
    
    /**
     * @return the comboSeq
     */
    public String getComboSeq() {
        return this.comboSeq;
    }
    
    /**
     * @param comboSeq the comboSeq to set
     */
    public void setComboSeq(final String comboSeq) {
        this.comboSeq = comboSeq;
    }
    
    /**
     * @return the resultVerstion
     */
    public int getResultVerstion() {
        return this.resultVerstion;
    }
    
    /**
     * @param resultVerstion the resultVerstion to set
     */
    public void setResultVerstion(final int resultVerstion) {
        this.resultVerstion = resultVerstion;
    }
    
    /**
     * @return the gapDays
     */
    public int getGapDays() {
        return this.gapDays;
    }
    
    /**
     * @param gapDays the gapDays to set
     */
    public void setGapDays(final int gapDays) {
        this.gapDays = gapDays;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PatientStageCombinationCount [personId=" + this.personId + ", comboIds=" + this.comboIds + ", comboSeq="
                + this.comboSeq + ", stage=" + this.stage + ", startDate=" + this.startDate + ", endDate=" + this.endDate
                + ", duration=" + this.duration + ", resultVerstion=" + this.resultVerstion + ", gapDays=" + this.gapDays
                + "]";
    }
}
