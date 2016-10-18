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

import org.ohdsi.webapi.panacea.pojo.PanaceaPatientSequenceCount;
import org.springframework.batch.item.ItemProcessor;

/**
 *
 */
public class PanaceaPatientSequenceCountItemProcessor implements ItemProcessor<PanaceaPatientSequenceCount, String> {
    
    /**
     * @see org.springframework.batch.item.ItemProcessor#process(java.lang.Object)
     */
    @Override
    public String process(final PanaceaPatientSequenceCount item) throws Exception {
        return item.toString();
    }
    
}
