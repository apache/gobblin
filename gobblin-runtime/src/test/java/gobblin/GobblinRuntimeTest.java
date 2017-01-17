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
package gobblin;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.runtime.local.LocalJobLauncher;


/**
 * Integration test for skipped work units.
 *
 * Created by adsharma on 11/22/16.
 */
public class GobblinRuntimeTest {
  private static final String RESOURCE_DIR = "./gobblin-runtime/src/test/resources/runtime_test/";

  public void invokeLocalJobLauncher(Properties properties)
      throws Exception {
    try (LocalJobLauncher localJobLauncher = new LocalJobLauncher(properties)) {
      localJobLauncher.launchJob(null);
    }
  }

  public Properties getJobProperties(String fileProperties)
      throws IOException {
    Properties jobProperties = new Properties();
    jobProperties.load(getClass().getClassLoader().getResourceAsStream(fileProperties));
    return jobProperties;
  }

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    FileUtils.forceMkdir(new File(RESOURCE_DIR + "state_store"));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + "writer_staging"));
    FileUtils.forceMkdir(new File(RESOURCE_DIR + "writer_output"));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + "state_store"));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + "writer_staging"));
    FileUtils.cleanDirectory(new File(RESOURCE_DIR + "writer_output"));
  }

  /**
   * This test is to validate that the skipped work units wont be passed to the publisher.
   * @throws Exception
   */
  @Test
  public void testSkippedWorkUnitsAvoidPublisher()
      throws Exception {
    Properties jobProperties = getJobProperties("runtime_test/skip_workunits_test.properties");
    jobProperties.setProperty("data.publisher.type", "gobblin.TestSkipWorkUnitsPublisher");
    invokeLocalJobLauncher(jobProperties);
  }

  /**
   * This test is to validate that job will be successful if commit policy is commit on full success, even if some of the workunits are skipped.
   * @throws Exception
   */
  @Test
  public void testJobSuccessOnFullCommit()
      throws Exception {
    Properties jobProperties = getJobProperties("runtime_test/skip_workunits_test.properties");
    jobProperties.setProperty("job.commit.policy", "full");
    invokeLocalJobLauncher(jobProperties);
  }

  /**
   * This test validates that the skipped work units are persisted in the state store and can be read again.
   * @throws Exception
   */
  @Test
  public void testSkippedWorkUnitsPersistenceInStateStore()
      throws Exception {
    Properties jobProperties = getJobProperties("runtime_test/skip_workunits_test.properties");
    invokeLocalJobLauncher(jobProperties);
    jobProperties.setProperty("test.workunit.persistence", "true");
    invokeLocalJobLauncher(jobProperties);
  }
}
