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

import java.io.PrintWriter;
import java.io.StringWriter;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class SingleTaskRunnerMainOptionsTest {

  private static final String TEST_JOB_ID = "1";
  private static final String TEST_WORKUNIT = "/workunit.wu";
  private static final String TEST_JOBSTATE_JOB_STATE = "/jobstate.job.state";
  private static final String TEST_CLUSTER_CONF = "/cluster.conf";
  private PrintWriter writer;
  private StringWriter stringWriter;

  @BeforeMethod
  public void setUp() {
    this.stringWriter = new StringWriter();
    this.writer = new PrintWriter(this.stringWriter, true);
  }

  @Test
  public void correctCmdLineShouldReturnAllValues() {
    final String[] args =
        {"--job_id", TEST_JOB_ID, "--work_unit_file_path", TEST_WORKUNIT, "--job_state_file_path", TEST_JOBSTATE_JOB_STATE, "--cluster_config_file_path", TEST_CLUSTER_CONF};
    final SingleTaskRunnerMainOptions options = new SingleTaskRunnerMainOptions(args, this.writer);
    final String jobId = options.getJobId();
    final String workUnitFilePath = options.getWorkUnitFilePath();
    final String jobStateFilePath = options.getJobStateFilePath();
    final String clusterConfigFilePath = options.getClusterConfigFilePath();

    assertThat(jobId).isEqualTo(TEST_JOB_ID);
    assertThat(workUnitFilePath).isEqualTo(TEST_WORKUNIT);
    assertThat(jobStateFilePath).isEqualTo(TEST_JOBSTATE_JOB_STATE);
    assertThat(clusterConfigFilePath).isEqualTo(TEST_CLUSTER_CONF);
  }

  @Test
  public void missingOptionShouldThrow() {
    final String[] args = {};

    assertThatThrownBy(() -> new SingleTaskRunnerMainOptions(args, this.writer))
        .isInstanceOf(GobblinClusterException.class);

    final String output = this.stringWriter.toString();
    assertThat(output).contains("usage: SingleTaskRunnerMain");
  }
}
