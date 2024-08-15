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

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;


/**
 * Tests the state updates (including updating in-memory state and MysqlDagActionStore) after performing add or cancel
 * operations by calling addDag, stopDag, kill, and resume. It also tests flows with and without sla configs.
 */
public class DagFlowTest {

  @Test
  void slaConfigCheck() throws Exception {
    Dag<JobExecutionPlan> dag = DagTestUtils.buildDag("5", 123456783L, "FINISH_RUNNING", 1);
    Assert.assertEquals(DagUtils.getFlowSLA(dag.getStartNodes().get(0)), ServiceConfigKeys.DEFAULT_FLOW_SLA_MILLIS);

    Config jobConfig = dag.getStartNodes().get(0).getValue().getJobSpec().getConfig();
    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef("7"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.SECONDS.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);
    Assert.assertEquals(DagUtils.getFlowSLA(dag.getStartNodes().get(0)), TimeUnit.SECONDS.toMillis(7L));

    jobConfig = jobConfig
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME, ConfigValueFactory.fromAnyRef("8"))
        .withValue(ConfigurationKeys.GOBBLIN_FLOW_SLA_TIME_UNIT, ConfigValueFactory.fromAnyRef(TimeUnit.MINUTES.name()));
    dag.getStartNodes().get(0).getValue().getJobSpec().setConfig(jobConfig);
    Assert.assertEquals(DagUtils.getFlowSLA(dag.getStartNodes().get(0)), TimeUnit.MINUTES.toMillis(8L));
  }
}
