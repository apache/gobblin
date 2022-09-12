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

package org.apache.gobblin.service.monitoring;

import java.util.List;

import com.google.common.base.Supplier;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.gobblin.runtime.troubleshooter.Issue;


/**
 * Contains attributes that describe job status.
 */
@Builder
@Getter
@ToString
public class JobStatus {
  private final String jobName;
  private final String jobGroup;
  private final String jobTag;
  private final long jobExecutionId;
  private final long flowExecutionId;
  private final String flowName;
  private final String flowGroup;
  private final String eventName;
  private final long orchestratedTime;
  private final long startTime;
  private final long endTime;
  @Setter
  private String metrics;
  private final String message;
  private final long processedCount;
  private final String lowWatermark;
  private final String highWatermark;
  private final int maxAttempts;
  private final int currentAttempts;
  private final int currentGeneration;
  private final boolean shouldRetry;
  private final Supplier<List<Issue>> issues;
  private final int progressPercentage;
  private final long lastProgressEventTime;
}
