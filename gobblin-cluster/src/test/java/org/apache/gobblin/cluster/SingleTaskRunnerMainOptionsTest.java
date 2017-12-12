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

import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_CLUSTER_CONF;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_JOB_ID;
import static org.apache.gobblin.cluster.SingleTaskRunnerMainArgumentsDataProvider.TEST_WORKUNIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SingleTaskRunnerMainOptionsTest {

  private PrintWriter writer;
  private StringWriter stringWriter;

  @BeforeMethod
  public void setUp() {
    this.stringWriter = new StringWriter();
    this.writer = new PrintWriter(this.stringWriter, true);
  }

  @Test
  public void correctCmdLineShouldReturnAllValues() {
    final String[] args = SingleTaskRunnerMainArgumentsDataProvider.getArgs();
    final SingleTaskRunnerMainOptions options = new SingleTaskRunnerMainOptions(args, this.writer);
    final String jobId = options.getJobId();
    final String workUnitFilePath = options.getWorkUnitFilePath();
    final String clusterConfigFilePath = options.getClusterConfigFilePath();

    assertThat(jobId).isEqualTo(TEST_JOB_ID);
    assertThat(workUnitFilePath).isEqualTo(TEST_WORKUNIT);
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
