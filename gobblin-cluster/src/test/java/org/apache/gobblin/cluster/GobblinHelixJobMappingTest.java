/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.cluster;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;


public class GobblinHelixJobMappingTest {

  @Test
  void testMapJobNameWithFlowExecutionId() {
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1234");
    props.setProperty(ConfigurationKeys.JOB_NAME_KEY, "job1");
    String planningJobId = HelixJobsMapping.createPlanningJobId(props);
    String actualJobId = HelixJobsMapping.createActualJobId(props);
    // The jobID contains the system timestamp that we need to parse out
    Assert.assertEquals(planningJobId.substring(0, planningJobId.lastIndexOf("_")), "job_PlanningJobjob1_1234");
    Assert.assertEquals(actualJobId.substring(0, actualJobId.lastIndexOf("_")), "job_ActualJobjob1_1234");
  }

  @Test
  void testMapJobNameWithoutFlowExecutionId() {
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.JOB_NAME_KEY, "job1");
    String planningJobId = HelixJobsMapping.createPlanningJobId(props);
    String actualJobId = HelixJobsMapping.createActualJobId(props);
    // The jobID contains the system timestamp that we need to parse out
    Assert.assertEquals(planningJobId.substring(0, planningJobId.lastIndexOf("_")), "job_PlanningJobjob1");
    Assert.assertEquals(actualJobId.substring(0, actualJobId.lastIndexOf("_")), "job_ActualJobjob1");
  }
}
