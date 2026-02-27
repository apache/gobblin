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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.util.ReadOnlyStringMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * Log4j 2.x appender that captures WARN and ERROR level events from the service/orchestration layer
 * and converts them to troubleshooting Issues.
 *
 * <p>Extracts flow context from MDC to associate issues with specific flow executions.
 *
 * <p>When targetFlowExecutionId is specified, only captures events from that specific execution,
 * preventing cross-flow contamination when multiple DagProcs run concurrently.
 *
 * @see ServiceLayerTroubleshooter
 */
@Slf4j
public class ServiceLayerLog4j2Appender extends AbstractAppender {

  private static final String AUTO_GENERATED_HASH_PREFIX = "S";
  private static final int AUTO_GENERATED_HASH_LENGTH = 6;

  private final IssueRepository repository;
  private final boolean requireMdcContext;
  private final int maxIssuesPerExecution;

  private final String targetFlowGroup;
  private final String targetFlowName;
  private final String targetFlowExecutionId;

  private final AtomicLong processedEventCount = new AtomicLong(0);
  private final AtomicLong skippedEventCount = new AtomicLong(0);
  private final AtomicLong capturedIssueCount = new AtomicLong(0);

  /**
   * @param requireMdcContext If true, skip events without valid MDC context
   * @param maxIssuesPerExecution Maximum issues to capture per execution
   * @param targetFlowGroup Only capture events from this flow group (null = no filtering)
   * @param targetFlowName Only capture events from this flow name (null = no filtering)
   * @param targetFlowExecutionId Only capture events from this execution (null = no filtering)
   */
  public ServiceLayerLog4j2Appender(String name, Filter filter, IssueRepository repository,
      boolean requireMdcContext, int maxIssuesPerExecution,
      String targetFlowGroup, String targetFlowName, String targetFlowExecutionId) {
    super(name, filter, null, true, Property.EMPTY_ARRAY);
    this.repository = repository;
    this.requireMdcContext = requireMdcContext;
    this.maxIssuesPerExecution = maxIssuesPerExecution;
    this.targetFlowGroup = targetFlowGroup;
    this.targetFlowName = targetFlowName;
    this.targetFlowExecutionId = targetFlowExecutionId;
  }

  public long getProcessedEventCount() {
    return processedEventCount.get();
  }

  public long getSkippedEventCount() {
    return skippedEventCount.get();
  }

  public long getCapturedIssueCount() {
    return capturedIssueCount.get();
  }

  @Override
  public void append(LogEvent event) {
    if (!event.getLevel().isMoreSpecificThan(Level.WARN)) {
      return;
    }

    processedEventCount.incrementAndGet();

    // Extract MDC context from LogEvent
    ReadOnlyStringMap contextData = event.getContextData();
    String flowGroup = contextData != null ? contextData.getValue(ConfigurationKeys.FLOW_GROUP_KEY) : null;
    String flowName = contextData != null ? contextData.getValue(ConfigurationKeys.FLOW_NAME_KEY) : null;
    String flowExecutionId = contextData != null ? contextData.getValue(ConfigurationKeys.FLOW_EXECUTION_ID_KEY) : null;

    // Skip events without valid MDC context if required
    if (requireMdcContext && !isMdcContextValid(flowGroup, flowName, flowExecutionId)) {
      skippedEventCount.incrementAndGet();
      return;
    }

    if (targetFlowGroup != null || targetFlowName != null || targetFlowExecutionId != null) {
      boolean matches = true;
      if (targetFlowGroup != null && !targetFlowGroup.equals(flowGroup)) {
        matches = false;
      }
      if (targetFlowName != null && !targetFlowName.equals(flowName)) {
        matches = false;
      }
      if (targetFlowExecutionId != null && !targetFlowExecutionId.equals(flowExecutionId)) {
        matches = false;
      }

      if (!matches) {
        skippedEventCount.incrementAndGet();
        return;
      }
    }

    // Enforce maximum issues limit
    if (capturedIssueCount.get() >= maxIssuesPerExecution) {
      skippedEventCount.incrementAndGet();
      return;
    }

    Issue issue = convertToIssue(event);

    try {
      repository.put(issue);
      capturedIssueCount.incrementAndGet();
    } catch (TroubleshooterException e) {
      log.warn("Failed to save issue to repository. Issue code: {}, time: {}", issue.getCode(), issue.getTime(), e);
    }
  }

  /**
   * Validates that MDC context contains required flow information.
   */
  private boolean isMdcContextValid(String flowGroup, String flowName, String flowExecutionId) {
    return !StringUtils.isEmpty(flowGroup)
        && !StringUtils.isEmpty(flowName)
        && !StringUtils.isEmpty(flowExecutionId)
        && !"0".equals(flowExecutionId);
  }

  /**
   * Converts a Log4j LogEvent to a Gobblin Issue.
   */
  private Issue convertToIssue(LogEvent event) {
    Issue.IssueBuilder issueBuilder = Issue.builder()
        .time(ZonedDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeMillis()), ZoneOffset.UTC))
        .severity(convertSeverity(event.getLevel()))
        .code(getIssueCode(event))
        .sourceClass(event.getLoggerName());

    if (event.getThrown() != null) {
      Throwable throwable = event.getThrown();
      issueBuilder.details(ExceptionUtils.getStackTrace(throwable));
      issueBuilder.exceptionClass(throwable.getClass().getName());

      // Create summary: "ExceptionMessage | LogMessage"
      String exceptionMessage = StringUtils.substringBefore(
          ExceptionUtils.getRootCauseMessage(throwable), System.lineSeparator());
      String logMessage = event.getMessage() != null ? event.getMessage().getFormattedMessage() : "";

      if (!StringUtils.isEmpty(logMessage)) {
        issueBuilder.summary(exceptionMessage + " | " + logMessage);
      } else {
        issueBuilder.summary(exceptionMessage);
      }
    } else {
      // No exception - just use log message
      issueBuilder.summary(event.getMessage() != null ? event.getMessage().getFormattedMessage() : "");
    }

    return issueBuilder.build();
  }

  /**
   * Generates an issue code from the logging event.
   */
  private String getIssueCode(LogEvent event) {
    if (event.getThrown() != null) {
      return getIssueCodeForException(event.getThrown());
    } else {
      // For non-exception logs, use logger name + message
      String source = event.getLoggerName() + ":" +
          (event.getMessage() != null ? event.getMessage().getFormattedMessage() : "");
      return AUTO_GENERATED_HASH_PREFIX +
          DigestUtils.sha256Hex(source).substring(0, AUTO_GENERATED_HASH_LENGTH).toUpperCase();
    }
  }

  /**
   * Generates an issue code from an exception's stack trace.
   */
  private String getIssueCodeForException(Throwable throwable) {
    StringBuilder stackTraceForHash = new StringBuilder();

    // Include all exceptions in the causal chain
    for (Throwable t : ExceptionUtils.getThrowableList(throwable)) {
      // Include exception class name
      stackTraceForHash.append(t.getClass().getName()).append("\n");

      // Include stack trace elements (class.method:line)
      for (StackTraceElement element : t.getStackTrace()) {
        stackTraceForHash.append(element.toString()).append("\n");
      }

      stackTraceForHash.append("---\n");
    }

    String hash = DigestUtils.sha256Hex(stackTraceForHash.toString());
    return AUTO_GENERATED_HASH_PREFIX + hash.substring(0, AUTO_GENERATED_HASH_LENGTH).toUpperCase();
  }

  /**
   * Converts Log4j 2.x Level to Gobblin IssueSeverity.
   */
  private IssueSeverity convertSeverity(Level level) {
    if (level == null) {
      return IssueSeverity.INFO;
    }

    if (level.isMoreSpecificThan(Level.FATAL)) {
      return IssueSeverity.FATAL;
    } else if (level.isMoreSpecificThan(Level.ERROR)) {
      return IssueSeverity.ERROR;
    } else if (level.isMoreSpecificThan(Level.WARN)) {
      return IssueSeverity.WARN;
    } else if (level.isMoreSpecificThan(Level.INFO)) {
      return IssueSeverity.INFO;
    } else {
      return IssueSeverity.DEBUG;
    }
  }
}
