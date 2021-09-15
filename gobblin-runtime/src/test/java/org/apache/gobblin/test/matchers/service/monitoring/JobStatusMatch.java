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

package org.apache.gobblin.test.matchers.service.monitoring;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import org.apache.gobblin.service.monitoring.JobStatus;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/** {@link org.hamcrest.Matcher} for {@link org.apache.gobblin.service.monitoring.JobStatus} */
@AllArgsConstructor(staticName = "ofTagged")
@RequiredArgsConstructor(staticName = "of")
@ToString
public class JobStatusMatch extends TypeSafeMatcher<JobStatus> {

  private final String flowGroup;
  private final String flowName;
  private final long flowExecutionId;

  private final String jobGroup;
  private final String jobName;
  private final long jobExecutionId;

  private final String eventName;
  private String jobTag;

  /** relative identification: acquire/share the `flowGroup`, `flowName`, and `flowExecutionId` of whichever dependent {@link #upon} */
  @AllArgsConstructor(staticName = "ofTagged")
  @RequiredArgsConstructor(staticName = "of")
  @ToString
  public static class Dependent {
    private final String jobGroup;
    private final String jobName;
    private final long jobExecutionId;
    private final String eventName;
    private String jobTag;

    public JobStatusMatch upon(FlowStatusMatch fsm) {
      return JobStatusMatch.ofTagged(fsm.getFlowGroup(), fsm.getFlowName(), fsm.getFlowExecutionId(), jobGroup, jobName, jobExecutionId, eventName, jobTag);
    }
  }

  /** supplements {@link #of} and {@link #ofTagged} factories, to simplify matching of "flow-level" `JobStatus` */
  public static JobStatusMatch ofFlowLevelStatus(String flowGroup, String flowName, long flowExecutionId, String eventName) {
    return of(flowGroup, flowName, flowExecutionId, JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY, 0L, eventName);
  }

  @Override
  public void describeTo(final Description description) {
    description.appendText("matches JobStatus of `" + toString() + "`");
  }

  @Override
  public boolean matchesSafely(JobStatus jobStatus) {
    return jobStatus.getFlowGroup().equals(flowGroup)
        && jobStatus.getFlowName().equals(flowName)
        && jobStatus.getFlowExecutionId() == flowExecutionId
        && jobStatus.getJobGroup().equals(jobGroup)
        && jobStatus.getJobName().equals(jobName)
        && jobStatus.getJobExecutionId() == jobExecutionId
        && jobStatus.getEventName().equals(eventName)
        && (jobStatus.getJobTag() == null ? jobTag == null : jobStatus.getJobTag().equals(jobTag));
  }
}
