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

package org.apache.gobblin.service.modules.orchestration.troubleshooter;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.MDC;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;

import static org.mockito.Mockito.*;


public class ServiceLayerTroubleshooterTest {

  @BeforeMethod
  public void setUp() {
    MDC.clear();
  }

  @AfterMethod
  public void tearDown() {
    MDC.clear();
  }

  @Test
  public void testStartAndStopAppender() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    Assert.assertFalse(troubleshooter.isStarted());

    troubleshooter.start();
    Assert.assertTrue(troubleshooter.isStarted());

    troubleshooter.stop();
    Assert.assertFalse(troubleshooter.isStarted());
  }

  @Test
  public void testReportIssuesDoesNotThrowWhenNoIssues() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    troubleshooter.stop();

    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    verify(mockSubmitter, never()).submit(any(IssueEventBuilder.class));
  }

  @Test
  public void testStartIsIdempotent() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    troubleshooter.start();

    Assert.assertTrue(troubleshooter.isStarted());
    troubleshooter.stop();
  }

  @Test
  public void testStopIsIdempotent() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    troubleshooter.stop();
    troubleshooter.stop();

    Assert.assertFalse(troubleshooter.isStarted());
  }

  @Test
  public void testClearWorks() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    Assert.assertEquals(troubleshooter.getIssueCount(), 0);

    troubleshooter.clear();
    Assert.assertEquals(troubleshooter.getIssueCount(), 0);
  }

  @Test
  public void testLogIssueSummaryDoesNotThrow() throws Exception {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);
    troubleshooter.logIssueSummary();
  }

  @Test
  public void testReportIssuesWithMdcContext() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "123456");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test-job");

    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    Issue testIssue = Issue.builder()
        .code("TEST001")
        .summary("Test issue")
        .build();

    troubleshooter.getIssueRepository().put(testIssue);
    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    ArgumentCaptor<IssueEventBuilder> captor = ArgumentCaptor.forClass(IssueEventBuilder.class);
    verify(mockSubmitter, times(1)).submit(captor.capture());

    IssueEventBuilder eventBuilder = captor.getValue();
    Map<String, String> metadata = eventBuilder.getMetadata();

    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD), "test-group");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD), "test-flow");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), "123456");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.JOB_NAME_FIELD), "test-job");
    Assert.assertEquals(metadata.get("issueSource"), "service-layer");
  }

  @Test
  public void testReportIssuesHandlesEventSubmitterFailure() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");

    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    doThrow(new RuntimeException("Submission failed")).when(mockSubmitter).submit(any(IssueEventBuilder.class));

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    Issue testIssue = Issue.builder()
        .code("TEST001")
        .summary("Test issue")
        .build();

    troubleshooter.getIssueRepository().put(testIssue);
    troubleshooter.reportIssuesAsEvents(mockSubmitter);
  }

  @Test
  public void testIssueCountReflectsRepositoryState() throws Exception {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    Assert.assertEquals(troubleshooter.getIssueCount(), 0);

    Issue issue1 = Issue.builder().code("TEST001").summary("Issue 1").build();
    Issue issue2 = Issue.builder().code("TEST002").summary("Issue 2").build();

    troubleshooter.getIssueRepository().put(issue1);
    Assert.assertEquals(troubleshooter.getIssueCount(), 1);

    troubleshooter.getIssueRepository().put(issue2);
    Assert.assertEquals(troubleshooter.getIssueCount(), 2);

    troubleshooter.clear();
    Assert.assertEquals(troubleshooter.getIssueCount(), 0);
  }
}
