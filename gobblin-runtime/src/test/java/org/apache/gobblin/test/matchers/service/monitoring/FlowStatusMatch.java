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

import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.collection.IsIterableContainingInOrder;

import com.google.api.client.util.Lists;
import com.google.common.collect.Iterables;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.monitoring.FlowStatus;
import org.apache.gobblin.service.monitoring.JobStatus;


/** {@link org.hamcrest.Matcher} for {@link org.apache.gobblin.service.monitoring.FlowStatus} */
@AllArgsConstructor(staticName = "withDependentJobStatuses")
@RequiredArgsConstructor(staticName = "of")
@ToString
public class FlowStatusMatch extends TypeSafeMatcher<FlowStatus> {
  @Getter
  private final String flowGroup;
  @Getter
  private final String flowName;
  @Getter
  private final long flowExecutionId;
  private final ExecutionStatus execStatus;
  private List<JobStatusMatch.Dependent> jsmDependents;

  @Override
  public void describeTo(final Description description) {
    description.appendText("matches FlowStatus of `" + toString() + "`");
  }

  @Override
  public boolean matchesSafely(FlowStatus flowStatus) {
    JobStatusMatch flowJobStatusMatch = JobStatusMatch.ofFlowLevelStatus(flowGroup, flowName, flowExecutionId, execStatus.name());
    List<Matcher<? super JobStatus>> matchers = new java.util.ArrayList<>();
    matchers.add(flowJobStatusMatch);
    if (jsmDependents != null) {
      jsmDependents.stream().map(dependent -> dependent.upon(this)).forEach(matchers::add);
    }
    return flowStatus.getFlowGroup().equals(flowGroup)
        && flowStatus.getFlowName().equals(flowName)
        && flowStatus.getFlowExecutionId() == flowExecutionId
        && assertOrderedJobStatuses(flowStatus, Iterables.toArray(matchers, Matcher.class));
  }

  @SafeVarargs
  private static boolean assertOrderedJobStatuses(FlowStatus flowStatus, Matcher<? super JobStatus>... matchers) {
    MatcherAssert.assertThat(Lists.newArrayList(flowStatus.getJobStatusIterator()),
        IsIterableContainingInOrder.contains(matchers));
    return true; // NOTE: exception thrown in case of error
  }
}
