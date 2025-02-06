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


package org.apache.gobblin.cluster.event;

import lombok.Getter;
import org.apache.gobblin.runtime.JobState;


/**
 * The `JobFailureEvent` class represents an event that is triggered when a job fails.
 * It contains information about the job state and a summary of the issues that caused the failure.
 */
public class JobFailureEvent {
  @Getter
  private final JobState jobState;
  @Getter
  private final String issuesSummary;
  public JobFailureEvent(JobState jobState, String issuesSummary) {
    this.jobState = jobState;
    this.issuesSummary = issuesSummary;
  }
}
