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

package org.apache.gobblin.runtime.troubleshooter;

import org.testng.annotations.Test;

import org.apache.gobblin.metrics.event.TimingEvent;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class JobIssueEventHandlerTest {

  @Test
  public void canHandleIssue()
      throws Exception {
    MultiContextIssueRepository issueRepository = mock(MultiContextIssueRepository.class);
    JobIssueEventHandler eventHandler = new JobIssueEventHandler(issueRepository, true);

    IssueEventBuilder eventBuilder = new IssueEventBuilder("TestJob");
    eventBuilder.setIssue(getTestIssue("test issue", "code1"));
    eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "test-group");
    eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "test-flow");
    eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1234");
    eventBuilder.addMetadata(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, "test-job");

    eventHandler.processEvent(eventBuilder.build());

    verify(issueRepository).put(any(), (Issue) any());
  }

  private Issue getTestIssue(String summary, String code) {
    return Issue.builder().summary(summary).code(code).build();
  }
}