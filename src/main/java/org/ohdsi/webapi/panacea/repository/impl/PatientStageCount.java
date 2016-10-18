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
@XmlRootElement(name = "PatientStageCount")
@XmlAccessorType(XmlAccessType.FIELD)
public class PatientStageCount implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private Long personId;
    
    private Long cmbId;
    
    private Date startDate;
    
    private Date endDate;
    
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
     * @return the startDate
     */
    public Date getStartDate() {
        return this.startDate;
    }
    
    /**
     * @param startDate the startDate to set
     */
    public void setStartDate(final Date startDate) {
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
        this.endDate = endDate;
    }
    
    /**
     * @return the cmbId
     */
    public Long getCmbId() {
        return this.cmbId;
    }
    
    /**
     * @param cmbId the cmbId to set
     */
    public void setCmbId(final Long cmbId) {
        this.cmbId = cmbId;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PatientStageCount [personId=" + this.personId + ", cmbId=" + this.cmbId + ", startDate=" + this.startDate
                + ", endDate=" + this.endDate + "]";
    }
    
}
