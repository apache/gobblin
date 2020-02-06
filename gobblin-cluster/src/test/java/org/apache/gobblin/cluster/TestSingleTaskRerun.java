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

import org.junit.Assert;
import org.testng.annotations.Test;


/**
 * Notes & Usage:
 * 0. This test could be used to reproduce task-execution issue in Gobblin-Cluster, within each container.
 * 1. The workunit is being drafted in {@link InMemoryWuFailedSingleTask}.
 * 2. When needed to reproduce certain errors, replace org.apache.gobblin.cluster.DummySource.DummyExtractor or
 * {@link DummySource} to plug in required logic.
 */
public class TestSingleTaskRerun {

  /**
   * An in-memory {@link SingleTask} runner that could be used to simulate how it works in Gobblin-Cluster.
   * For this example method, it fail the execution by missing certain configuration on purpose, catch the exception and
   * re-run it again.
   */
  @Test
  public void testMetricObjectCasting()
      throws Exception {
    final String clusterConfigPath = "clusterConf";
    final String wuPath = "_workunits/store/workunit.wu";
    String clusterConfPath = this.getClass().getClassLoader().getResource(clusterConfigPath).getPath();

    InMemorySingleTaskRunner inMemorySingleTaskRunner =
        new InMemorySingleTaskRunner(clusterConfPath, "testJob",
            this.getClass().getClassLoader().getResource(wuPath).getPath());
    try {
      inMemorySingleTaskRunner.run(true);
    } catch (Exception e) {
      inMemorySingleTaskRunner.run();
    }

    Assert.assertTrue(true);
  }
}
