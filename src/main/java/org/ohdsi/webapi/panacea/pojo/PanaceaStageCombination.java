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
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 */
//@Entity(name = "PanaceaStageCombination")
//@Table(name = "pnc_tx_stage_combination")
@XmlRootElement(name = "PanaceaStageCombination")
@XmlAccessorType(XmlAccessType.FIELD)
public class PanaceaStageCombination implements Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    //@SequenceGenerator(name = "PNC_TX_STG_CMB_SEQUENCE_GENERATOR", sequenceName = "seq_pnc_tx_stg_cmb", allocationSize = 1)
    //@Id
    //@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "PNC_TX_STG_CMB_SEQUENCE_GENERATOR")
    //@GeneratedValue(strategy = GenerationType.AUTO, generator = "PNC_TX_STG_CMB_SEQUENCE_GENERATOR")
    //@Column(name = "pnc_tx_stg_cmb_id")
    private Long pncTxStgCmbId;
    
    //@Column(name = "study_id")
    private Long studyId;
    
    //@OneToMany(targetEntity = PanaceaStageCombinationMap.class, fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    //@JoinColumn(name = "pnc_tx_stg_cmb_id", referencedColumnName = "pnc_tx_stg_cmb_id")
    //@ElementCollection(targetClass = PanaceaStageCombinationMap.class)
    //@JsonManagedReference
    private List<PanaceaStageCombinationMap> combMapList;
    
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
     * @return the combMapList
     */
    public List<PanaceaStageCombinationMap> getCombMapList() {
        return this.combMapList;
    }
    
    /**
     * @param combMapList the combMapList to set
     */
    public void setCombMapList(final List<PanaceaStageCombinationMap> combMapList) {
        this.combMapList = combMapList;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "PanaceaStageCombination [pncTxStgCmbId=" + this.pncTxStgCmbId + ", studyId=" + this.studyId + "]";
    }
}
