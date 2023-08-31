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

import com.google.inject.Singleton;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.modules.orchestration.proc.AdvanceDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.CleanUpDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.DagProc;
import org.apache.gobblin.service.modules.orchestration.proc.KillDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.LaunchDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ReloadDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ResumeDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.RetryDagProc;
import org.apache.gobblin.service.modules.orchestration.task.AdvanceDagTask;
import org.apache.gobblin.service.modules.orchestration.task.CleanUpDagTask;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReloadDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.orchestration.task.RetryDagTask;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Factory for creating {@link DagProc} based on the visitor type for a given {@link DagTask}.
 */

@Alpha
@Slf4j
@Singleton
public class DagProcFactory implements DagTaskVisitor {
  Optional<EventSubmitter> eventSubmitter;

  public DagProcFactory(boolean instrumentationEnabled) {
    if (instrumentationEnabled) {
      MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build());
    } else {
      this.eventSubmitter = Optional.empty();
    }
  }

  @Override
  public DagProc meet(LaunchDagTask launchDagTask, DagProcessingEngine dagProcessingEngine) {
    return new LaunchDagProc(launchDagTask, dagProcessingEngine);
  }

  @Override
  public DagProc meet(KillDagTask killDagTask, DagProcessingEngine dagProcessingEngine) {
    return new KillDagProc(killDagTask, dagProcessingEngine);
  }

  @Override
  public Object meet(ReloadDagTask reloadDagTask, DagProcessingEngine dagProcessingEngine) {
    return new ReloadDagProc(reloadDagTask, dagProcessingEngine);
  }

  @Override
  public DagProc meet(ResumeDagTask resumeDagTask, DagProcessingEngine dagProcessingEngine) {
    return new ResumeDagProc(resumeDagTask, dagProcessingEngine);
  }

  @Override
  public DagProc meet(RetryDagTask retryDagTask, DagProcessingEngine dagProcessingEngine) {
    return new RetryDagProc(retryDagTask, dagProcessingEngine);
  }

  @Override
  public DagProc meet(AdvanceDagTask advanceDagTask, DagProcessingEngine dagProcessingEngine) {
    return new AdvanceDagProc(advanceDagTask, dagProcessingEngine);
  }

  @Override
  public DagProc meet(CleanUpDagTask cleanUpDagTask, DagProcessingEngine dagProcessingEngine) {
    return new CleanUpDagProc(cleanUpDagTask, dagProcessingEngine);
  }
}

