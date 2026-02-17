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

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.MDC;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.InMemoryIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * Unit tests for {@link ServiceLayerLogAppender}.
 */
public class ServiceLayerLogAppenderTest {

  private InMemoryIssueRepository repository;
  private ServiceLayerLogAppender appender;
  private Logger testLogger;

  @BeforeMethod
  public void setUp() {
    repository = new InMemoryIssueRepository();
    testLogger = Logger.getLogger("ServiceLayerLogAppenderTest");
    testLogger.setLevel(Level.ALL);
  }

  @AfterMethod
  public void tearDown() {
    if (appender != null) {
      testLogger.removeAppender(appender);
    }
    MDC.clear();
  }

  @Test
  public void testCapturesErrorWithFullMdcContext() throws TroubleshooterException {
    // Setup MDC with complete flow context
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "123456");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test-job");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Trigger error
    RuntimeException testException = new RuntimeException("Test exception message");
    testLogger.error("Test error log message", testException);

    // Verify issue captured
    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1, "Should have captured exactly one issue");

    Issue issue = issues.get(0);
    Assert.assertEquals(issue.getSeverity(), IssueSeverity.ERROR, "Issue severity should be ERROR");
    Assert.assertTrue(issue.getSummary().contains("Test exception message"), "Issue summary should contain exception message");
    Assert.assertTrue(issue.getSummary().contains("Test error log message"), "Issue summary should contain log message");
    Assert.assertTrue(issue.getCode().startsWith("S"), "Issue code should start with 'S'");
    Assert.assertEquals(issue.getCode().length(), 7, "Issue code length should be 7 (S + 6 chars)");
    Assert.assertTrue(issue.getDetails().contains("RuntimeException"), "Issue details should contain stack trace");
    Assert.assertEquals(issue.getExceptionClass(), "java.lang.RuntimeException", "Exception class should be set");
  }

  @Test
  public void testSkipsEventsWhenMdcMissing() throws TroubleshooterException {
    // Do NOT set MDC
    appender = new ServiceLayerLogAppender(repository, true, 100); // requireMdcContext = true
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Trigger error
    testLogger.error("Error without MDC");

    // Verify NO issue captured
    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 0, "Should not capture issues when MDC is missing");
    Assert.assertEquals(appender.getSkippedEventCount(), 1, "Should have skipped one event");
  }

  @Test
  public void testCapturesEventsWhenMdcNotRequired() throws TroubleshooterException {
    // Do NOT set MDC
    appender = new ServiceLayerLogAppender(repository, false, 100); // requireMdcContext = false
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Trigger error
    testLogger.error("Error without MDC");

    // Verify issue IS captured (MDC not required)
    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1, "Should capture issues even when MDC is missing (if not required)");
  }

  @Test
  public void testIssueCodeDeterministic() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Create exception with fixed stack trace
    RuntimeException ex = new RuntimeException("Same error");
    StackTraceElement[] stackTrace = new StackTraceElement[] {
        new StackTraceElement("com.test.Class", "method", "file.java", 123)
    };
    ex.setStackTrace(stackTrace);

    // Log same exception
    testLogger.error("Error with fixed exception", ex);

    List<Issue> issues = repository.getAll();
    Assert.assertTrue(issues.size() >= 1, "Should have captured at least one issue");

    // Verify the issue code is deterministic (starts with S and has correct length)
    String code = issues.get(0).getCode();
    Assert.assertTrue(code.startsWith("S"), "Issue code should start with S");
    Assert.assertEquals(code.length(), 7, "Issue code should be 7 characters (S + 6 hex chars)");
  }

  @Test
  public void testDifferentExceptionsProduceDifferentCodes() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Log two different exceptions
    testLogger.error("Error 1", new RuntimeException("First error"));
    testLogger.error("Error 2", new IllegalArgumentException("Second error"));

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 2, "Should have captured two issues");

    String code1 = issues.get(0).getCode();
    String code2 = issues.get(1).getCode();
    Assert.assertNotEquals(code1, code2, "Different exceptions should produce different issue codes");
  }

  @Test
  public void testSeverityConversion() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.DEBUG); // Capture all levels
    testLogger.addAppender(appender);

    // Log different severity levels
    testLogger.warn("Warning message");
    testLogger.error("Error message");

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 2, "Should have captured two issues");

    // Find WARN and ERROR issues
    Issue warnIssue = null;
    Issue errorIssue = null;
    for (Issue issue : issues) {
      if (issue.getSeverity() == IssueSeverity.WARN) {
        warnIssue = issue;
      } else if (issue.getSeverity() == IssueSeverity.ERROR) {
        errorIssue = issue;
      }
    }

    Assert.assertNotNull(warnIssue, "Should have captured WARN issue");
    Assert.assertNotNull(errorIssue, "Should have captured ERROR issue");
    Assert.assertTrue(warnIssue.getSummary().contains("Warning message"), "WARN issue should have warning message");
    Assert.assertTrue(errorIssue.getSummary().contains("Error message"), "ERROR issue should have error message");
  }

  @Test
  public void testLogWithoutExceptionCaptured() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Log error without exception
    testLogger.error("Simple error message without exception");

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1, "Should have captured one issue");

    Issue issue = issues.get(0);
    Assert.assertEquals(issue.getSummary(), "Simple error message without exception", "Issue summary should be the log message");
    Assert.assertNull(issue.getExceptionClass(), "Exception class should be null");
  }

  @Test
  public void testSourceClassSetToLoggerName() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    testLogger.error("Test error");

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1, "Should have captured one issue");

    Issue issue = issues.get(0);
    Assert.assertEquals(issue.getSourceClass(), "ServiceLayerLogAppenderTest", "Source class should be logger name");
  }

  @Test
  public void testSkipsEventsWhenFlowExecutionIdIsZero() throws TroubleshooterException {
    // Invalid flow execution ID (0 is invalid)
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "0"); // Invalid!
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test-job");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    testLogger.error("Error with invalid flow execution ID");

    // Should skip due to invalid context
    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 0, "Should not capture issues when flow execution ID is 0");
    Assert.assertEquals(appender.getSkippedEventCount(), 1, "Should have skipped one event");
  }

  @Test
  public void testNestedExceptionChainIncludedInDetails() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Create nested exception
    RuntimeException rootCause = new RuntimeException("Root cause exception");
    IllegalStateException wrappedException = new IllegalStateException("Wrapper exception", rootCause);

    testLogger.error("Nested exception test", wrappedException);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1, "Should have captured one issue");

    Issue issue = issues.get(0);
    String details = issue.getDetails();
    Assert.assertTrue(details.contains("Root cause exception"), "Details should contain root cause");
    Assert.assertTrue(details.contains("Wrapper exception"), "Details should contain wrapper exception");
    Assert.assertTrue(details.contains("IllegalStateException") && details.contains("RuntimeException"),
        "Details should contain stack trace for both exceptions");
  }

  @Test
  public void testProcessedEventCountIncremented() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    appender = new ServiceLayerLogAppender(repository, true, 100);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    Assert.assertEquals(appender.getProcessedEventCount(), 0, "Initial processed count should be 0");

    testLogger.error("Error 1");
    testLogger.warn("Warning 1");
    testLogger.error("Error 2");

    Assert.assertEquals(appender.getProcessedEventCount(), 3, "Processed count should be 3");
    Assert.assertEquals(repository.getAll().size(), 3, "Should have captured 3 issues");
  }

  @Test
  public void testMaxIssuesLimitEnforced() throws TroubleshooterException {
    MDC.put(ConfigurationKeys.FLOW_GROUP_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_NAME_KEY, "test");
    MDC.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "1");
    MDC.put(ConfigurationKeys.JOB_NAME_KEY, "test");

    // Create appender with max limit of 5 issues
    appender = new ServiceLayerLogAppender(repository, true, 5);
    appender.setThreshold(Level.WARN);
    testLogger.addAppender(appender);

    // Log 10 errors - only first 5 should be captured
    for (int i = 1; i <= 10; i++) {
      testLogger.error("Error " + i);
    }

    // Verify only 5 issues were captured
    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 5, "Should have captured exactly 5 issues (max limit)");
    Assert.assertEquals(appender.getCapturedIssueCount(), 5, "Captured count should be 5");
    Assert.assertEquals(appender.getProcessedEventCount(), 10, "Processed count should be 10");
    Assert.assertEquals(appender.getSkippedEventCount(), 5, "Skipped count should be 5 (exceeded max limit)");

    // Verify the first 5 issues are the ones captured
    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(issues.get(i).getSummary().contains("Error " + (i + 1)),
          "Issue " + i + " should contain correct error message");
    }
  }
}
