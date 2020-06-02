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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;


/**
 * Notes & Usage:
 * 0. This test could be used to reproduce task-execution issue in Gobblin-Cluster, within each container. It doesn't
 * interact with Helix framework in any sense but focus on GobblinTask execution itself. In particular, {@link InMemorySingleTaskRunner}
 * execute {@link SingleTask#run()} directly so that the whole execution and commit stack of
 * {@link org.apache.gobblin.runtime.GobblinMultiTaskAttempt} will be covered.
 * 1. The workunit is being created in {@link InMemoryWuFailedSingleTask}.
 * 2. When needed to reproduce certain errors, replace org.apache.gobblin.cluster.DummySource.DummyExtractor or
 * {@link DummySource} to plug in required logic.
 */
public class TestSingleTask {

  private InMemorySingleTaskRunner createInMemoryTaskRunner()
      throws IOException {
    final File clusterWorkDirPath = Files.createTempDir();
    Path clusterConfigPath = Paths.get(clusterWorkDirPath.getAbsolutePath(), "clusterConf");
    Config config = ConfigFactory.empty().withValue(GobblinTaskRunner.CLUSTER_APP_WORK_DIR, ConfigValueFactory.fromAnyRef(clusterWorkDirPath.toString()));
    ConfigUtils configUtils = new ConfigUtils(new FileUtils());
    configUtils.saveConfigToFile(config, clusterConfigPath);

    final Path wuPath = Paths.get(clusterWorkDirPath.getAbsolutePath(), "_workunits/store/workunit.wu");

    InMemorySingleTaskRunner inMemorySingleTaskRunner =
        new InMemorySingleTaskRunner(clusterConfigPath.toString(), "testJob", wuPath.toString());
    return inMemorySingleTaskRunner;
  }

  /**
   * An in-memory {@link SingleTask} runner that could be used to simulate how it works in Gobblin-Cluster.
   * For this example method, it fail the execution by missing certain configuration on purpose, catch the exception and
   * re-run it again.
   */
  @Test
  public void testSingleTaskRerunAfterFailure() throws Exception {
    InMemorySingleTaskRunner inMemorySingleTaskRunner = createInMemoryTaskRunner();
    try {
      inMemorySingleTaskRunner.run(true);
    } catch (Exception e) {
      inMemorySingleTaskRunner.run();
    }
    Assert.assertTrue(true);
  }
}
