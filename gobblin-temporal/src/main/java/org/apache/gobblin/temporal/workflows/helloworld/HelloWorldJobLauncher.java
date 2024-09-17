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

package org.apache.gobblin.temporal.workflows.helloworld;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;

import com.google.common.eventbus.EventBus;

import io.temporal.client.WorkflowOptions;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobLauncher;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;



/**
 * An implementation of {@link JobLauncher} that launches a Gobblin job using the Temporal task framework.
 *
 * <p>
 *   This class is instantiated by the {@link GobblinTemporalJobScheduler#buildJobLauncher(Properties)} on every job submission to launch the Gobblin job.
 *   The actual task execution happens in the {@link GobblinTemporalTaskRunner}, usually in a different process.
 * </p>
 */
@Alpha
@Slf4j
public class HelloWorldJobLauncher extends GobblinTemporalJobLauncher {
  public HelloWorldJobLauncher(Properties jobProps, Path appWorkDir, List<? extends Tag<?>> metadataTags,
      ConcurrentHashMap<String, Boolean> runningMap, EventBus eventBus)
      throws Exception {
    super(jobProps, appWorkDir, metadataTags, runningMap, eventBus);
  }

  @Override
  public void submitJob(List<WorkUnit> workunits) {
    WorkflowOptions options = WorkflowOptions.newBuilder().setTaskQueue(queueName).setSearchAttributes(TemporalWorkFlowUtils.generateGaasSearchAttributes(this.jobProps)).build();
    GreetingWorkflow greetingWorkflow = this.client.newWorkflowStub(GreetingWorkflow.class, options);
    EventSubmitterContext eventSubmitterContext = new EventSubmitterContext.Builder(eventSubmitter).build();
    String greeting = greetingWorkflow.getGreeting("Gobblin", eventSubmitterContext);
    log.info(greeting);
  }
}
