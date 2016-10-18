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
package org.ohdsi.webapi.test;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ohdsi.webapi.WebApi;
import org.ohdsi.webapi.panacea.repository.impl.PanaceaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = WebApi.class)
@Ignore
public class PanaceaJobTest extends TestCase {
    
    private static final Log log = LogFactory.getLog(PanaceaJobTest.class);
    
    @Autowired
    private PanaceaService pncService;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    //    @Test
    //    public void testCreateJob() {
    //        final Job pncJob = this.jobConfig.getPanaceaJob();
    //        
    //        log.info("PanaceaJobTest.testJob: " + pncJob.toString());
    //    }
    
    @Ignore
    public void testLaunchJob() {
        //        this.pncService.runJob(new Long(18));
        //this.pncService.runTestJob();
        /**
         * test with browser too: http://localhost:8080/WebAPI/panacea/testpncjob
         */
        this.pncService.runTestPanaceaJob(new Long(18), new Integer(1));
    }
    
    @Test
    public void testTasklet() {
        /**
         * test: localhost:8080/WebAPI/panacea/runPncTasklet/CCAE/18
         */
        /**
         * set break point in JobTemplate.launchJob() at line of
         * "exec = this.jobLauncher.run(job, jobParameters)" to get around the thread pool executor
         * problem for unit test.
         */
        this.pncService.runPanaceaTasklet("CCAE", new Long(18));
    }
}
