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

import java.io.IOException;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.cluster.suite.IntegrationBasicSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedManagerClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationDedicatedTaskDriverClusterSuite;
import org.apache.gobblin.cluster.suite.IntegrationJobFactorySuite;
import org.apache.gobblin.cluster.suite.IntegrationJobTagSuite;
import org.apache.gobblin.cluster.suite.IntegrationSeparateProcessSuite;


public class ClusterIntegrationTest {

  private IntegrationBasicSuite suite;

  @Test
  public void testJobShouldComplete()
      throws Exception {
    this.suite = new IntegrationBasicSuite();
    runAndVerify();
  }

  @Test
  public void testSeparateProcessMode()
      throws Exception {
    this.suite = new IntegrationSeparateProcessSuite();
    runAndVerify();
  }

  @Test
  public void testDedicatedManagerCluster()
      throws Exception {
    this.suite = new IntegrationDedicatedManagerClusterSuite();
    runAndVerify();
  }

  @Test(enabled = false)
  public void testDedicatedTaskDriverCluster()
      throws Exception {
    this.suite = new IntegrationDedicatedTaskDriverClusterSuite();
    runAndVerify();
  }

  @Test(enabled = false)
  public void testJobWithTag()
      throws Exception {
    this.suite = new IntegrationJobTagSuite();
    runAndVerify();
  }

  @Test
  public void testPlanningJobFactory()
      throws Exception {
    this.suite = new IntegrationJobFactorySuite();
    runAndVerify();
  }

  private void runAndVerify()
      throws Exception {
    suite.startCluster();
    suite.waitForAndVerifyOutputFiles();
    suite.shutdownCluster();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    this.suite.deleteWorkDir();
  }
}
