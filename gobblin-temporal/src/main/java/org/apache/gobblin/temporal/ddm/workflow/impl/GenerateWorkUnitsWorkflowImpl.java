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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.time.Duration;
import java.util.Properties;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.ddm.activity.GenerateWorkUnits;
import org.apache.gobblin.temporal.ddm.work.GenerateWorkUnitsResult;
import org.apache.gobblin.temporal.ddm.workflow.GenerateWorkUnitsWorkflow;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;


@Slf4j
public class GenerateWorkUnitsWorkflowImpl implements GenerateWorkUnitsWorkflow {
  public static final Duration startToCloseTimeout = Duration.ofMinutes(90); // TODO: make configurable

  private static final RetryOptions ACTIVITY_RETRY_OPTS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final ActivityOptions ACTIVITY_OPTS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(startToCloseTimeout)
      .setRetryOptions(ACTIVITY_RETRY_OPTS)
      .build();

  private final GenerateWorkUnits activityStub = Workflow.newActivityStub(GenerateWorkUnits.class, ACTIVITY_OPTS);

  @Override
  public GenerateWorkUnitsResult generate(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    return activityStub.generateWorkUnits(jobProps, eventSubmitterContext);
  }
}
