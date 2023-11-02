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

import java.util.Optional;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.orchestration.proc.AdvanceDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.CleanUpDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.proc.KillDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ResumeDagProc;
import org.apache.gobblin.service.modules.orchestration.task.AdvanceDagTask;
import org.apache.gobblin.service.modules.orchestration.task.CleanUpDagTask;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Factory for creating {@link DagProc} based on the visitor type for a given {@link DagTask}.
 */

@Alpha
@Slf4j
public class DagProcFactory implements DagTaskVisitor<DagProc> {

  private DagManagementStateStore dagManagementStateStore;
  private JobStatusRetriever jobStatusRetriever;
  private FlowStatusGenerator flowStatusGenerator;
  private UserQuotaManager quotaManager;
  private SpecCompiler specCompiler;
  private FlowCatalog flowCatalog;
  private FlowCompilationValidationHelper flowCompilationValidationHelper;
  private Config config;
  private Optional<EventSubmitter> eventSubmitter;
  private boolean instrumentationEnabled;

  public DagProcFactory(Config config, DagManagementStateStore dagManagementStateStore, JobStatusRetriever jobStatusRetriever,
      FlowStatusGenerator flowStatusGenerator, UserQuotaManager quotaManager, SpecCompiler specCompiler, FlowCatalog flowCatalog, FlowCompilationValidationHelper flowCompilationValidationHelper, boolean instrumentationEnabled) {

    this.config = config;
    this.dagManagementStateStore = dagManagementStateStore;
    this.jobStatusRetriever = jobStatusRetriever;
    this.flowStatusGenerator = flowStatusGenerator;
    this.quotaManager = quotaManager;
    this.specCompiler = specCompiler;
    this.flowCatalog = flowCatalog;
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
    this.instrumentationEnabled = instrumentationEnabled;
    MetricContext metricContext = null;
    if (instrumentationEnabled) {
      metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    } else {
      this.eventSubmitter = Optional.empty();
    }
  }


  @Override
  public DagProc meet(LaunchDagTask launchDagTask) {
    return new LaunchDagProc(launchDagTask);
  }

  @Override
  public DagProc meet(KillDagTask killDagTask) {
    return new KillDagProc(killDagTask);
  }

  @Override
  public DagProc meet(ResumeDagTask resumeDagTask) {
    return new ResumeDagProc(resumeDagTask);
  }

  @Override
  public DagProc meet(AdvanceDagTask advanceDagTask) {
    return new AdvanceDagProc(advanceDagTask);
  }

  @Override
  public DagProc meet(CleanUpDagTask cleanUpDagTask) {
    return new CleanUpDagProc(cleanUpDagTask);
  }
}

