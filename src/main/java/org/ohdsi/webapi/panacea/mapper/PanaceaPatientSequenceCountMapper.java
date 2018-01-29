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

import org.ohdsi.webapi.panacea.pojo.PanaceaPatientSequenceCount;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 */
public class PanaceaPatientSequenceCountMapper implements RowMapper {
    
    /**
     * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
     */
    @Override
    public PanaceaPatientSequenceCount mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final PanaceaPatientSequenceCount ppsc = new PanaceaPatientSequenceCount();
        
        //TODO -- test only!!!!!!!!!!!
        ppsc.setPersonId(rs.getLong("study_id"));
        //        ppsc.setPersonId(rs.getLong("person_id"));
        //        ppsc.setConceptId(rs.getLong("drug_concept_id"));
        //        ppsc.setConceptName(rs.getString("concept_name"));
        //        ppsc.setStudyId(rs.getLong("study_id"));
        //        ppsc.setDurationDay(rs.getInt("duration"));
        //        ppsc.setStartDate(rs.getDate("drug_era_start_date"));
        //        ppsc.setEndDate(rs.getDate("drug_era_end_date"));
        
        return ppsc;
    }
    
}
