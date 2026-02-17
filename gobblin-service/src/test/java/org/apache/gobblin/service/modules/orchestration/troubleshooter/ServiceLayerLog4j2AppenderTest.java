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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.SortedArrayStringMap;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.InMemoryIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;


public class ServiceLayerLog4j2AppenderTest {

  private InMemoryIssueRepository repository;

  @BeforeMethod
  public void setUp() {
    repository = new InMemoryIssueRepository();
  }

  @Test
  public void testAppenderFiltersInfoLevel() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    LogEvent infoEvent = createLogEvent(Level.INFO, "Info message", null);
    appender.append(infoEvent);

    Assert.assertEquals(repository.getAll().size(), 0);
    Assert.assertEquals(appender.getProcessedEventCount(), 0);
  }

  @Test
  public void testAppenderCapturesWarnLevel() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Map<String, String> contextData = createContextData("test-group", "test-flow", "123");
    LogEvent warnEvent = createLogEvent(Level.WARN, "Warning message", null, contextData);
    appender.append(warnEvent);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1);
    Assert.assertEquals(issues.get(0).getSeverity(), IssueSeverity.WARN);
    Assert.assertTrue(issues.get(0).getSummary().contains("Warning message"));
  }

  @Test
  public void testAppenderCapturesErrorLevel() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Map<String, String> contextData = createContextData("test-group", "test-flow", "123");
    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error message", null, contextData);
    appender.append(errorEvent);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1);
    Assert.assertEquals(issues.get(0).getSeverity(), IssueSeverity.ERROR);
  }

  @Test
  public void testAppenderCapturesExceptionDetails() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Exception exception = new RuntimeException("Test exception");
    Map<String, String> contextData = createContextData("test-group", "test-flow", "123");
    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error with exception", exception, contextData);
    appender.append(errorEvent);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1);

    Issue issue = issues.get(0);
    Assert.assertEquals(issue.getExceptionClass(), "java.lang.RuntimeException");
    Assert.assertTrue(issue.getDetails().contains("RuntimeException: Test exception"));
    Assert.assertTrue(issue.getSummary().contains("Test exception"));
    Assert.assertTrue(issue.getSummary().contains("Error with exception"));
  }

  @Test
  public void testAppenderGeneratesIssueCode() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Map<String, String> contextData = createContextData("test-group", "test-flow", "123");
    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error message", null, contextData);
    appender.append(errorEvent);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1);

    String code = issues.get(0).getCode();
    Assert.assertNotNull(code);
    Assert.assertTrue(code.startsWith("S"));
    Assert.assertEquals(code.length(), 7);
  }

  @Test
  public void testAppenderSkipsEventsWithoutMdcWhenRequired() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(true, 10, null, null, null);
    appender.start();

    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error without MDC", null, new HashMap<>());
    appender.append(errorEvent);

    Assert.assertEquals(repository.getAll().size(), 0);
    Assert.assertEquals(appender.getProcessedEventCount(), 1);
    Assert.assertEquals(appender.getSkippedEventCount(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 0);
  }

  @Test
  public void testAppenderCapturesEventsWithoutMdcWhenNotRequired() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error without MDC", null, new HashMap<>());
    appender.append(errorEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
  }

  @Test
  public void testAppenderSkipsInvalidExecutionId() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(true, 10, null, null, null);
    appender.start();

    Map<String, String> contextData = createContextData("test-group", "test-flow", "0");
    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error message", null, contextData);
    appender.append(errorEvent);

    Assert.assertEquals(repository.getAll().size(), 0);
    Assert.assertEquals(appender.getSkippedEventCount(), 1);
  }

  @Test
  public void testAppenderEnforcesMaxIssuesLimit() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 3, null, null, null);
    appender.start();

    Map<String, String> contextData = createContextData("test-group", "test-flow", "123");
    for (int i = 0; i < 5; i++) {
      LogEvent errorEvent = createLogEvent(Level.ERROR, "Error " + i, null, contextData);
      appender.append(errorEvent);
    }

    Assert.assertEquals(repository.getAll().size(), 3);
    Assert.assertEquals(appender.getProcessedEventCount(), 5);
    Assert.assertEquals(appender.getCapturedIssueCount(), 3);
    Assert.assertEquals(appender.getSkippedEventCount(), 2);
  }

  @Test
  public void testAppenderFlowGroupFiltering() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, "target-group", null, null);
    appender.start();

    Map<String, String> matchingContext = createContextData("target-group", "flow1", "123");
    LogEvent matchingEvent = createLogEvent(Level.ERROR, "Matching error", null, matchingContext);
    appender.append(matchingEvent);

    Map<String, String> nonMatchingContext = createContextData("other-group", "flow1", "123");
    LogEvent nonMatchingEvent = createLogEvent(Level.ERROR, "Non-matching error", null, nonMatchingContext);
    appender.append(nonMatchingEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
    Assert.assertEquals(appender.getSkippedEventCount(), 1);
  }

  @Test
  public void testAppenderFlowNameFiltering() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, "target-flow", null);
    appender.start();

    Map<String, String> matchingContext = createContextData("group1", "target-flow", "123");
    LogEvent matchingEvent = createLogEvent(Level.ERROR, "Matching error", null, matchingContext);
    appender.append(matchingEvent);

    Map<String, String> nonMatchingContext = createContextData("group1", "other-flow", "123");
    LogEvent nonMatchingEvent = createLogEvent(Level.ERROR, "Non-matching error", null, nonMatchingContext);
    appender.append(nonMatchingEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
  }

  @Test
  public void testAppenderExecutionIdFiltering() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, "123");
    appender.start();

    Map<String, String> matchingContext = createContextData("group1", "flow1", "123");
    LogEvent matchingEvent = createLogEvent(Level.ERROR, "Matching error", null, matchingContext);
    appender.append(matchingEvent);

    Map<String, String> nonMatchingContext = createContextData("group1", "flow1", "456");
    LogEvent nonMatchingEvent = createLogEvent(Level.ERROR, "Non-matching error", null, nonMatchingContext);
    appender.append(nonMatchingEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
  }

  @Test
  public void testAppenderMultipleFlowFilters() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, "target-group", "target-flow", "123");
    appender.start();

    Map<String, String> matchingContext = createContextData("target-group", "target-flow", "123");
    LogEvent matchingEvent = createLogEvent(Level.ERROR, "Matching error", null, matchingContext);
    appender.append(matchingEvent);

    Map<String, String> partialMatch1 = createContextData("target-group", "target-flow", "456");
    LogEvent partialEvent1 = createLogEvent(Level.ERROR, "Partial match 1", null, partialMatch1);
    appender.append(partialEvent1);

    Map<String, String> partialMatch2 = createContextData("target-group", "other-flow", "123");
    LogEvent partialEvent2 = createLogEvent(Level.ERROR, "Partial match 2", null, partialMatch2);
    appender.append(partialEvent2);

    Map<String, String> noMatch = createContextData("other-group", "other-flow", "789");
    LogEvent noMatchEvent = createLogEvent(Level.ERROR, "No match", null, noMatch);
    appender.append(noMatchEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
    Assert.assertEquals(appender.getSkippedEventCount(), 3);
  }

  @Test
  public void testAppenderNoFilteringWhenTargetNull() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Map<String, String> context1 = createContextData("group1", "flow1", "123");
    LogEvent event1 = createLogEvent(Level.ERROR, "Error 1", null, context1);
    appender.append(event1);

    Map<String, String> context2 = createContextData("group2", "flow2", "456");
    LogEvent event2 = createLogEvent(Level.ERROR, "Error 2", null, context2);
    appender.append(event2);

    Assert.assertEquals(repository.getAll().size(), 2);
    Assert.assertEquals(appender.getCapturedIssueCount(), 2);
  }

  @Test
  public void testAppenderExtractsContextFromLogEvent() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    Map<String, String> contextData = new HashMap<>();
    contextData.put(ConfigurationKeys.FLOW_GROUP_KEY, "test-group");
    contextData.put(ConfigurationKeys.FLOW_NAME_KEY, "test-flow");
    contextData.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "123456");
    contextData.put("other-key", "other-value");

    LogEvent errorEvent = createLogEvent(Level.ERROR, "Test error", null, contextData);
    appender.append(errorEvent);

    List<Issue> issues = repository.getAll();
    Assert.assertEquals(issues.size(), 1);
    Assert.assertEquals(appender.getCapturedIssueCount(), 1);
  }

  @Test
  public void testAppenderHandlesEmptyContextData() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(false, 10, null, null, null);
    appender.start();

    LogEvent errorEvent = createLogEvent(Level.ERROR, "Error message", null, null);
    appender.append(errorEvent);

    Assert.assertEquals(repository.getAll().size(), 1);
  }

  @Test
  public void testAppenderCountersAreAccurate() throws Exception {
    ServiceLayerLog4j2Appender appender = createAppender(true, 5, null, null, null);
    appender.start();

    LogEvent infoEvent = createLogEvent(Level.INFO, "Info", null);
    appender.append(infoEvent);

    Map<String, String> validContext = createContextData("group", "flow", "123");
    LogEvent validError = createLogEvent(Level.ERROR, "Valid", null, validContext);
    appender.append(validError);

    LogEvent invalidContextError = createLogEvent(Level.ERROR, "Invalid", null, new HashMap<>());
    appender.append(invalidContextError);

    for (int i = 0; i < 10; i++) {
      LogEvent error = createLogEvent(Level.ERROR, "Error " + i, null, validContext);
      appender.append(error);
    }

    Assert.assertEquals(appender.getProcessedEventCount(), 12);
    Assert.assertEquals(appender.getCapturedIssueCount(), 5);
    Assert.assertEquals(appender.getSkippedEventCount(), 7);
  }

  private ServiceLayerLog4j2Appender createAppender(boolean requireMdcContext, int maxIssues,
      String targetFlowGroup, String targetFlowName, String targetFlowExecutionId) {
    return new ServiceLayerLog4j2Appender(
        "TestAppender",
        null,
        repository,
        requireMdcContext,
        maxIssues,
        targetFlowGroup,
        targetFlowName,
        targetFlowExecutionId
    );
  }

  private LogEvent createLogEvent(Level level, String message, Throwable throwable) {
    return createLogEvent(level, message, throwable, null);
  }

  private LogEvent createLogEvent(Level level, String message, Throwable throwable, Map<String, String> contextData) {
    SortedArrayStringMap contextMap = contextData != null ? new SortedArrayStringMap(contextData) : new SortedArrayStringMap();

    return Log4jLogEvent.newBuilder()
        .setLoggerName(getClass().getName())
        .setLevel(level)
        .setMessage(new SimpleMessage(message))
        .setThrown(throwable)
        .setTimeMillis(Instant.now().toEpochMilli())
        .setContextData(contextMap)
        .build();
  }

  private Map<String, String> createContextData(String flowGroup, String flowName, String flowExecutionId) {
    Map<String, String> contextData = new HashMap<>();
    contextData.put(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    contextData.put(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    contextData.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
    return contextData;
  }
}
