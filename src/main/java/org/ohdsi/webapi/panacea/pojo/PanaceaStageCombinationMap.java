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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonBackReference;

/**
 *
 */
//@Entity(name = "PanaceaStageCombinationMap")
//@Table(name = "pnc_tx_stage_combination_map")
@XmlRootElement(name = "PanaceaStageCombinationMap")
@XmlAccessorType(XmlAccessType.FIELD)
public class PanaceaStageCombinationMap implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    //@SequenceGenerator(name = "PNC_TX_STG_CMB_MAP_SEQUENCE_GENERATOR", sequenceName = "seq_pnc_tx_stg_cmb_mp", allocationSize = 1)
    //@Id
    //@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "PNC_TX_STG_CMB_MAP_SEQUENCE_GENERATOR")
    //@GeneratedValue(strategy = GenerationType.AUTO, generator = "PNC_TX_STG_CMB_MAP_SEQUENCE_GENERATOR")
    //@Column(name = "pnc_tx_stg_cmb_mp_id")
    private Long pncTxStgCmbMpId;
    
    private Long pncTxStgCmbId;
    
    //    @Column(name = "pnc_tx_stg_cmb_id")
    //    private Long pncTxStgCmbId;
    
    //@Column(name = "concept_id")
    private Long conceptId;
    
    //@Column(name = "concept_name")
    private String conceptName;
    
    //@ManyToOne
    //@JoinColumn(name = "pnc_tx_stg_cmb_id", nullable = false)
    @JsonBackReference
    private PanaceaStageCombination pncStgCmb;
    
    /**
     * @return the pncTxStgCmbMpId
     */
    public Long getPncTxStgCmbMpId() {
        return this.pncTxStgCmbMpId;
    }
    
    /**
     * @param pncTxStgCmbMpId the pncTxStgCmbMpId to set
     */
    public void setPncTxStgCmbMpId(final Long pncTxStgCmbMpId) {
        this.pncTxStgCmbMpId = pncTxStgCmbMpId;
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
     * @return the pncStgCmb
     */
    public PanaceaStageCombination getPncStgCmb() {
        return this.pncStgCmb;
    }
    
    /**
     * @param pncStgCmb the pncStgCmb to set
     */
    public void setPncStgCmb(final PanaceaStageCombination pncStgCmb) {
        this.pncStgCmb = pncStgCmb;
    }
    
    /**
     * @return the pncTxStgCmbId
     */
    public Long getPncTxStgCmbId() {
        return this.pncTxStgCmbId;
    }
    
    /**
     * @param pncTxStgCmbId the pncTxStgCmbId to set
     */
    public void setPncTxStgCmbId(final Long pncTxStgCmbId) {
        this.pncTxStgCmbId = pncTxStgCmbId;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PanaceaStageCombinationMap [pncTxStgCmbMpId=" + this.pncTxStgCmbMpId + ", pncTxStgCmbId="
                + this.pncTxStgCmbId + ", conceptId=" + this.conceptId + ", conceptName=" + this.conceptName
                + ", pncStgCmb=" + this.pncStgCmb + "]";
    }
}
