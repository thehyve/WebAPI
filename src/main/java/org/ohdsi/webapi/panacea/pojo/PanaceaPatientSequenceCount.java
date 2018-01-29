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
import java.sql.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 */
//@Entity(name = "PanaceaPatientSequenceCount")
//@Table(name = "pnc_tmp_pt_sq_ct")
@XmlRootElement(name = "PanaceaPatientSequenceCount")
@XmlAccessorType(XmlAccessType.FIELD)
public class PanaceaPatientSequenceCount implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    //@SequenceGenerator(name = "PANACEA_PT_SQ_CT_SEQUENCE_GENERATOR", sequenceName = "seq_pnc_tmp_pt_sq_ct", allocationSize = 1)
    //@Id
    //@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "PANACEA_PT_SQ_CT_SEQUENCE_GENERATOR")
    //@GeneratedValue(strategy = GenerationType.AUTO, generator = "PANACEA_PT_SQ_CT_SEQUENCE_GENERATOR")
    //@Column(name = "pnc_pt_sq_ct_id")
    private Long patientSequenceCountId;
    
    //@Column(name = "study_id")
    private Long studyId;
    
    //@Column(name = "source_id")
    private Integer sourceId;
    
    //@Column(name = "person_id")
    private Long personId;
    
    //@Column(name = "tx_seq")
    private Long txSequence;
    
    //@Column(name = "concept_id")
    private Long conceptId;
    
    //@Column(name = "concept_name")
    private String conceptName;
    
    //@Column(name = "idx_start_date")
    private Date startDate;
    
    //@Column(name = "idx_end_date")
    private Date endDate;
    
    //@Column(name = "duration_days ")
    private Integer durationDay;
    
    /**
     * @return the patientSequenceCountId
     */
    public Long getPatientSequenceCountId() {
        return this.patientSequenceCountId;
    }
    
    /**
     * @param patientSequenceCountId the patientSequenceCountId to set
     */
    public void setPatientSequenceCountId(final Long patientSequenceCountId) {
        this.patientSequenceCountId = patientSequenceCountId;
    }
    
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
     * @return the txSequence
     */
    public Long getTxSequence() {
        return this.txSequence;
    }
    
    /**
     * @param txSequence the txSequence to set
     */
    public void setTxSequence(final Long txSequence) {
        this.txSequence = txSequence;
    }
    
    /**
     * @return the conceptId
     */
    public Long getConceptId() {
        return this.conceptId;
    }
    
    /**
     * @param conceptId the conceptId to set
     */
    public void setConceptId(final Long conceptId) {
        this.conceptId = conceptId;
    }
    
    /**
     * @return the conceptName
     */
    public String getConceptName() {
        return this.conceptName;
    }
    
    /**
     * @param conceptName the conceptName to set
     */
    public void setConceptName(final String conceptName) {
        this.conceptName = conceptName;
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
     * @return the durationDay
     */
    public Integer getDurationDay() {
        return this.durationDay;
    }
    
    /**
     * @param durationDay the durationDay to set
     */
    public void setDurationDay(final Integer durationDay) {
        this.durationDay = durationDay;
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
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PanaceaPatientSequenceCount [patientSequenceCountId=" + this.patientSequenceCountId + ", studyId="
                + this.studyId + ", sourceId=" + this.sourceId + ", personId=" + this.personId + ", txSequence="
                + this.txSequence + ", conceptId=" + this.conceptId + ", conceptName=" + this.conceptName + ", startDate="
                + this.startDate + ", endDate=" + this.endDate + ", durationDay=" + this.durationDay + "]";
    }
    
}
