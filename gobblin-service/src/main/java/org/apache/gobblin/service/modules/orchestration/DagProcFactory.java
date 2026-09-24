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

import java.util.Objects;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.proc.EnforceFlowFinishDeadlineDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.EnforceJobStartDeadlineDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.KillDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ReevaluateDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ResumeDagProc;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.EnforceFlowFinishDeadlineDagTask;
import org.apache.gobblin.service.modules.orchestration.task.EnforceJobStartDeadlineDagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

/**
 * {@link DagTaskVisitor} for transforming a specific {@link DagTask} derived class to its companion {@link DagProc} derived class.
 * Each {@link DagTask} needs it own {@link DagProcFactory#meet} method overload to create {@link DagProc} that is
 * supposed to process that {@link DagTask}.
 */

@Alpha
@Singleton
public class DagProcFactory implements DagTaskVisitor<DagProc<?>> {

  private final Config config;
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;
  private final Optional<Provider<ForceKillHandler>> forceKillHandler;
  private final Provider<JobStatusRetriever> jobStatusRetriever;

  public DagProcFactory(Config config, FlowCompilationValidationHelper flowCompilationValidationHelper) {
    this(config, flowCompilationValidationHelper, Optional.absent(), null);
  }

  @Inject
  public DagProcFactory(Config config, FlowCompilationValidationHelper flowCompilationValidationHelper,
      Optional<Provider<ForceKillHandler>> forceKillHandler, Provider<JobStatusRetriever> jobStatusRetriever) {
    this.config = config;
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
    this.forceKillHandler = Objects.requireNonNull(forceKillHandler, "forceKillHandler");
    this.jobStatusRetriever = forceKillHandler.isPresent()
        ? Objects.requireNonNull(jobStatusRetriever, "jobStatusRetriever") : jobStatusRetriever;
  }

  @Override
  public EnforceFlowFinishDeadlineDagProc meet(EnforceFlowFinishDeadlineDagTask enforceFlowFinishDeadlineDagTask) {
    return new EnforceFlowFinishDeadlineDagProc(enforceFlowFinishDeadlineDagTask, this.config);
  }

  @Override
  public EnforceJobStartDeadlineDagProc meet(EnforceJobStartDeadlineDagTask enforceJobStartDeadlineDagTask) {
    return new EnforceJobStartDeadlineDagProc(enforceJobStartDeadlineDagTask, this.config);
  }

  @Override
  public LaunchDagProc meet(LaunchDagTask launchDagTask) {
    return new LaunchDagProc(launchDagTask, this.flowCompilationValidationHelper, this.config);
  }

  @Override
  public ReevaluateDagProc meet(ReevaluateDagTask reEvaluateDagTask) {
    return new ReevaluateDagProc(reEvaluateDagTask, this.config);
  }

  @Override
  public KillDagProc meet(KillDagTask killDagTask) {
    if (this.forceKillHandler.isPresent()) {
      return new KillDagProc(killDagTask, this.config, this.forceKillHandler.get(), this.jobStatusRetriever);
    }
    return new KillDagProc(killDagTask, this.config);
  }

  @Override
  public ResumeDagProc meet(ResumeDagTask resumeDagTask) {
    return new ResumeDagProc(resumeDagTask, this.config);
  }
}
