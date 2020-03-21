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

package org.apache.gobblin.service.monitoring;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class FsJobStatusRetrieverTest extends JobStatusRetrieverTest {

  private String stateStoreDir = "/tmp/jobStatusRetrieverTest/statestore";

  @BeforeClass
  public void setUp() throws Exception {
    cleanUpDir();
    Config config = ConfigFactory.empty().withValue(FsJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        ConfigValueFactory.fromAnyRef(stateStoreDir));
    this.jobStatusRetriever = new FsJobStatusRetriever(config);
  }

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    super.testGetJobStatusesForFlowExecution();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution")
  public void testJobTiming() throws Exception {
    super.testJobTiming();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testOutOfOrderJobTimingEvents() throws IOException {
    super.testOutOfOrderJobTimingEvents();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testGetJobStatusesForFlowExecution1() {
    super.testGetJobStatusesForFlowExecution1();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution1")
  public void testGetLatestExecutionIdsForFlow() throws Exception {
    super.testGetLatestExecutionIdsForFlow();
  }

  @Override
  protected void cleanUpDir() throws Exception {
    File specStoreDir = new File(this.stateStoreDir);
    if (specStoreDir.exists()) {
      FileUtils.deleteDirectory(specStoreDir);
    }
  }
}