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

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

/**
 *
 */
public class PanaceaSummaryMapper implements RowMapper<PanaceaSummary> {
    
    /**
     * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
     */
    @Override
    public PanaceaSummary mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final PanaceaSummary ps = new PanaceaSummary();
        
        ps.setStudyId(rs.getLong("study_id"));
        //        ps.setSourceId(rs.getInt("source_id"));
        ps.setLastUpdateTime(rs.getTimestamp("last_update_time"));
        ps.setStudyResults(rs.getString("study_results"));
        ps.setStudyResultCollapsed(rs.getString("study_results_2"));
        ps.setStudyResultFiltered(rs.getString("study_results_filtered"));
        
        return ps;
    }
}
