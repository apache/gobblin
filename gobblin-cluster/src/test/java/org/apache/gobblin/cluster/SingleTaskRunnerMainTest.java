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

import org.testng.annotations.Test;

import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_CLUSTER_CONF;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_JOB_ID;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_WORKUNIT;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


public class SingleTaskRunnerMainTest {

  @Test
  public void testRun()
      throws Exception {
    final SingleTaskRunnerBuilder builder = spy(SingleTaskRunnerBuilder.class);
    final SingleTaskRunner taskRunner = mock(SingleTaskRunner.class);
    doReturn(taskRunner).when(builder).createSingleTaskRunner();

    final SingleTaskRunnerMain runnerMain = new SingleTaskRunnerMain(builder);
    runnerMain.run(SingleTaskRunnerMainArgumentsDataProvider.getArgs());

    verify(builder).setClusterConfigFilePath(TEST_CLUSTER_CONF);
    verify(builder).setJobId(TEST_JOB_ID);
    verify(builder).setWorkUnitFilePath(TEST_WORKUNIT);
    verify(taskRunner).run();
  }
}
