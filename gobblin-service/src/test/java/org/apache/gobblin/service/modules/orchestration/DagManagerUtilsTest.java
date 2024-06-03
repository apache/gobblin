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

package org.apache.gobblin.service.modules.orchestration;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


public class DagManagerUtilsTest {

  @Test
  public void testGetJobSpecFromDag() throws Exception {
    Dag<JobExecutionPlan> testDag = DagTestUtils.buildDag("testDag", 1000L);
    JobSpec jobSpec = DagManagerUtils.getJobSpec(testDag.getNodes().get(0));
    Assert.assertEquals(jobSpec.getConfigAsProperties().size(), jobSpec.getConfig().entrySet().size());
    for (String key : jobSpec.getConfigAsProperties().stringPropertyNames()) {
      Assert.assertTrue(jobSpec.getConfig().hasPath(key));
      // Assume each key is a string because all job configs are currently strings
      Assert.assertEquals(jobSpec.getConfigAsProperties().get(key), jobSpec.getConfig().getString(key));
    }
  }
}
