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
package org.ohdsi.webapi.panacea.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.ohdsi.webapi.panacea.pojo.PanaceaStageCombination;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 */
public class PanaceaStageCombinationMapper implements RowMapper<PanaceaStageCombination> {
    
    /**
     * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
     */
    @Override
    public PanaceaStageCombination mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final PanaceaStageCombination pncStgCmb = new PanaceaStageCombination();
        pncStgCmb.setPncTxStgCmbId(rs.getLong("PNC_TX_STG_CMB_ID"));
        pncStgCmb.setStudyId(rs.getLong("STUDY_ID"));
        return pncStgCmb;
    }
    
}
