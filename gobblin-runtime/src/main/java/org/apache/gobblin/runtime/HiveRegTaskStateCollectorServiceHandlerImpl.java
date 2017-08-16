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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Collection;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.publisher.HiveRegistrationPublisher;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link TaskStateCollectorServiceHandler} implementation that execute hive registration on driver level.
 * It registers all {@link TaskState} once they are available.
 * Since {@link TaskStateCollectorService} is by default being invoked every minute,
 * if a single batch of hive registration finishes within a minute, the latency can be hidden by the gap between two run
 * of {@link TaskStateCollectorService}.
 */
@Slf4j
public class HiveRegTaskStateCollectorServiceHandlerImpl implements TaskStateCollectorServiceHandler {

  private HiveRegistrationPublisher hiveRegHandler;

  private boolean isJobProceedOnCollectorServiceFailure;

  /**
   * By default, whether {@link TaskStateCollectorServiceHandler} finishes successfully or not won't influence
   * job's proceed.
   */
  private static final boolean defaultPolicyOnCollectorServiceFailure = true;

  public HiveRegTaskStateCollectorServiceHandlerImpl(JobState jobState){
    hiveRegHandler = new HiveRegistrationPublisher(jobState);
    isJobProceedOnCollectorServiceFailure = jobState.contains(ConfigurationKeys.JOB_PROCEED_ON_TASK_STATE_COLLECOTR_SERVICE_FAILURE)
        ? jobState.getPropAsBoolean(ConfigurationKeys.JOB_PROCEED_ON_TASK_STATE_COLLECOTR_SERVICE_FAILURE)
        : defaultPolicyOnCollectorServiceFailure;
  }

  @Override
  public void handle(Collection<? extends WorkUnitState> taskStates) {
    try {
      this.hiveRegHandler.publishData(taskStates);
    } catch (Throwable t) {
      if (isJobProceedOnCollectorServiceFailure) {
        log.error("Failed to commit dataset", t);
        SafeDatasetCommit.setTaskFailureException(taskStates, t);
      } else {
        throw new RuntimeException("Hive Registration as the TaskStateCollectorServiceHandler failed.", t);
      }
    }
  }

  @Override
  public void close() throws IOException {
    hiveRegHandler.close();
  }
}
