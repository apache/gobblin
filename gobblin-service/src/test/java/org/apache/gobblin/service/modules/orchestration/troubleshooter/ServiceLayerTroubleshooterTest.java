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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;

import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link ServiceLayerTroubleshooter}.
 */
public class ServiceLayerTroubleshooterTest {

  private static final Logger log = LoggerFactory.getLogger(ServiceLayerTroubleshooterTest.class);

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

    Assert.assertFalse(troubleshooter.isStarted(), "Should not be started initially");

    troubleshooter.start();
    Assert.assertTrue(troubleshooter.isStarted(), "Should be started after start()");

    troubleshooter.stop();
    Assert.assertFalse(troubleshooter.isStarted(), "Should not be started after stop()");
  }

  @Test
  public void testCapturesErrorsAfterStart() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "123456");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test-job");

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);
    troubleshooter.start();

    // Trigger error
    log.error("Test error from ServiceLayerTroubleshooterTest");

    troubleshooter.stop();

    // Verify issue captured
    Assert.assertEquals(troubleshooter.getIssueCount(), 1, "Should have captured one issue");
  }

  @Test
  public void testDoesNotCaptureErrorsBeforeStart() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    // Log error BEFORE starting troubleshooter
    log.error("Error before troubleshooter started");

    troubleshooter.start();
    troubleshooter.stop();

    // Should not have captured the error that was logged before start()
    Assert.assertEquals(troubleshooter.getIssueCount(), 0, "Should not capture errors before start()");
  }

  @Test
  public void testDoesNotCaptureErrorsAfterStop() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    log.error("Error during troubleshooter active");
    troubleshooter.stop();

    // Log error AFTER stopping troubleshooter
    log.error("Error after troubleshooter stopped");

    // Should only have captured the error logged while active
    Assert.assertEquals(troubleshooter.getIssueCount(), 1, "Should only capture errors while active");
  }

  @Test
  public void testReportIssuesAsEventsWithCorrectMetadata() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "123456");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test-job");

    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    log.error("Test service error");
    troubleshooter.stop();

    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    // Verify event submitted with correct metadata
    ArgumentCaptor<IssueEventBuilder> captor = ArgumentCaptor.forClass(IssueEventBuilder.class);
    verify(mockSubmitter, times(1)).submit(captor.capture());

    IssueEventBuilder eventBuilder = captor.getValue();
    Map<String, String> metadata = eventBuilder.getMetadata();

    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD), "test-group", "Flow group should match MDC");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD), "test-flow", "Flow name should match MDC");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), "123456", "Flow execution ID should match MDC");
    Assert.assertEquals(metadata.get(TimingEvent.FlowEventConstants.JOB_NAME_FIELD), "test-job", "Job name should match MDC");
    Assert.assertEquals(metadata.get("issueSource"), "service-layer", "Issue source should be service-layer");

    // Verify issue content
    Issue issue = eventBuilder.getIssue();
    Assert.assertNotNull(issue, "Issue should not be null");
    Assert.assertEquals(issue.getSeverity(), IssueSeverity.ERROR, "Issue severity should be ERROR");
    Assert.assertTrue(issue.getSummary().contains("Test service error"), "Issue summary should contain log message");
  }

  @Test
  public void testReportIssuesSubmitsMultipleEvents() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    log.error("Error 1");
    log.error("Error 2");
    log.warn("Warning 1");
    troubleshooter.stop();

    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    // Should submit 3 events (2 errors + 1 warning)
    verify(mockSubmitter, times(3)).submit(any(IssueEventBuilder.class));
  }

  @Test
  public void testReportIssuesHandlesMissingMdc() throws Exception {
    EventSubmitter mockSubmitter = Mockito.mock(EventSubmitter.class);
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(false, 100); // Don't require MDC

    troubleshooter.start();
    log.error("Error without MDC");
    troubleshooter.stop();

    // Should still report issues, even with missing MDC
    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    verify(mockSubmitter, times(1)).submit(any(IssueEventBuilder.class));
  }

  @Test
  public void testLogIssueSummaryDoesNotThrow() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    log.error("Test error for summary");
    troubleshooter.stop();

    // Should not throw exception
    troubleshooter.logIssueSummary();
  }

  @Test
  public void testClearRemovesAllIssues() throws Exception {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    log.error("Error 1");
    log.error("Error 2");
    troubleshooter.stop();

    Assert.assertEquals(troubleshooter.getIssueCount(), 2, "Should have captured 2 issues");

    troubleshooter.clear();
    Assert.assertEquals(troubleshooter.getIssueCount(), 0, "Should have no issues after clear");
  }

  @Test
  public void testStartIsIdempotent() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    troubleshooter.start(); // Second start() should be no-op

    Assert.assertTrue(troubleshooter.isStarted(), "Should still be started");
    troubleshooter.stop();
  }

  @Test
  public void testStopIsIdempotent() {
    ServiceLayerTroubleshooter troubleshooter = new ServiceLayerTroubleshooter(true, 100);

    troubleshooter.start();
    troubleshooter.stop();
    troubleshooter.stop(); // Second stop() should be no-op

    Assert.assertFalse(troubleshooter.isStarted(), "Should still be stopped");
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
    // No errors logged
    troubleshooter.stop();

    // Should not throw
    troubleshooter.reportIssuesAsEvents(mockSubmitter);

    // Should not call submit since there are no issues
    verify(mockSubmitter, never()).submit(any(IssueEventBuilder.class));
  }
}
