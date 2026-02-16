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
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.MDC;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * Log4j appender for service/orchestration layer that extracts job context from MDC (Mapped Diagnostic Context)
 * and converts log events to Issues. This is the service-layer equivalent of AutoTroubleshooterLogAppender.
 *
 * <p>Key differences from executor-side appender:
 * <ul>
 *   <li>Uses MDC for context extraction instead of JobState</li>
 *   <li>Issue codes prefixed with 'S' (Service layer) instead of 'T' (TemporalExecutor)</li>
 *   <li>Lifecycle tied to DagProc execution instead of job execution</li>
 * </ul>
 *
 * <p><b>MDC Requirements:</b> This appender requires the following MDC keys to be set:
 * <ul>
 *   <li>{@link ConfigurationKeys#FLOW_GROUP_KEY}</li>
 *   <li>{@link ConfigurationKeys#FLOW_NAME_KEY}</li>
 *   <li>{@link ConfigurationKeys#FLOW_EXECUTION_ID_KEY}</li>
 *   <li>{@link ConfigurationKeys#JOB_NAME_KEY} (optional, defaults to "NA")</li>
 * </ul>
 *
 * <p>If {@code requireMdcContext} is true and MDC context is missing, events are skipped
 * to prevent incorrect issue attribution.
 *
 * @see ServiceLayerTroubleshooter
 */
@Slf4j
public class ServiceLayerLogAppender extends AppenderSkeleton {

  /**
   * Prefix for auto-generated issue codes. 'S' indicates Service/orchestration layer,
   * distinguishing from T (Temporal executor) used by AutoTroubleshooterLogAppender.
   */
  private static final String AUTO_GENERATED_HASH_PREFIX = "S";

  /**
   * Length of the hash portion of auto-generated issue codes (e.g., "S3A7F2B").
   */
  private static final int AUTO_GENERATED_HASH_LENGTH = 6;

  /**
   * Repository for storing captured issues.
   */
  private final IssueRepository repository;

  /**
   * If true, only capture events when MDC context is fully populated.
   * If false, capture all events (MDC context is optional).
   */
  private final boolean requireMdcContext;

  /**
   * Counter for total events processed (for debugging/metrics).
   */
  private final AtomicLong processedEventCount = new AtomicLong(0);

  /**
   * Counter for events skipped due to missing MDC (for debugging/metrics).
   */
  private final AtomicLong skippedEventCount = new AtomicLong(0);

  /**
   * Maximum number of issues to capture per execution.
   */
  private final int maxIssuesPerExecution;

  /**
   * Counter for issues successfully captured.
   */
  private final AtomicLong capturedIssueCount = new AtomicLong(0);

  /**
   * Creates a new ServiceLayerLogAppender.
   *
   * @param repository Repository for storing captured issues
   * @param requireMdcContext If true, skip events when MDC context is missing
   * @param maxIssuesPerExecution Maximum number of issues to capture (prevents memory issues)
   */
  public ServiceLayerLogAppender(IssueRepository repository, boolean requireMdcContext, int maxIssuesPerExecution) {
    this.repository = repository;
    this.requireMdcContext = requireMdcContext;
    this.maxIssuesPerExecution = maxIssuesPerExecution;
  }

  /**
   * Returns the number of events processed by this appender.
   */
  public long getProcessedEventCount() {
    return processedEventCount.get();
  }

  /**
   * Returns the number of events skipped due to missing MDC context.
   */
  public long getSkippedEventCount() {
    return skippedEventCount.get();
  }

  /**
   * Returns the number of issues successfully captured.
   */
  public long getCapturedIssueCount() {
    return capturedIssueCount.get();
  }

  @Override
  protected void append(LoggingEvent event) {
    processedEventCount.incrementAndGet();

    // Validate MDC context is available
    String flowGroup = MDC.get(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = MDC.get(ConfigurationKeys.FLOW_NAME_KEY);
    String flowExecutionId = MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    if (requireMdcContext && !isMdcContextValid(flowGroup, flowName, flowExecutionId)) {
      skippedEventCount.incrementAndGet();
      log.debug("ServiceLayerLogAppender: Skipping event due to missing/invalid MDC context. " +
          "Logger: {}, Message: {}, flowGroup={}, flowName={}, flowExecutionId={}",
          event.getLoggerName(), event.getRenderedMessage(), flowGroup, flowName, flowExecutionId);
      return;
    }

    // Check if we've reached the maximum number of issues
    long currentCount = capturedIssueCount.get();
    if (currentCount >= maxIssuesPerExecution) {
      skippedEventCount.incrementAndGet();
      log.debug("ServiceLayerLogAppender: Maximum issues limit reached ({}/{}). Skipping event. " +
          "Logger: {}, Message: {}",
          currentCount, maxIssuesPerExecution, event.getLoggerName(), event.getRenderedMessage());
      return;
    }

    Issue issue = convertToIssue(event);

    try {
      repository.put(issue);
      capturedIssueCount.incrementAndGet();
    } catch (TroubleshooterException e) {
      log.warn("ServiceLayerLogAppender: Failed to save issue to repository. " +
          "Issue code: {}, time: {}", issue.getCode(), issue.getTime(), e);
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
   * Converts a Log4j LoggingEvent to a Gobblin Issue.
   * This method mirrors the behavior of AutoTroubleshooterLogAppender.convertToIssue().
   */
  private Issue convertToIssue(LoggingEvent event) {
    Issue.IssueBuilder issueBuilder = Issue.builder()
        .time(ZonedDateTime.ofInstant(Instant.ofEpochMilli(event.getTimeStamp()), ZoneOffset.UTC))
        .severity(convertSeverity(event.getLevel()))
        .code(getIssueCode(event))
        .sourceClass(event.getLoggerName());

    if (event.getThrowableInformation() != null) {
      Throwable throwable = event.getThrowableInformation().getThrowable();
      issueBuilder.details(ExceptionUtils.getStackTrace(throwable));
      issueBuilder.exceptionClass(throwable.getClass().getName());

      // Create summary: "ExceptionMessage | LogMessage"
      String exceptionMessage = StringUtils.substringBefore(
          ExceptionUtils.getRootCauseMessage(throwable), System.lineSeparator());
      String logMessage = event.getRenderedMessage() != null ? event.getRenderedMessage() : "";

      if (!StringUtils.isEmpty(logMessage)) {
        issueBuilder.summary(exceptionMessage + " | " + logMessage);
      } else {
        issueBuilder.summary(exceptionMessage);
      }
    } else {
      // No exception - just use log message
      issueBuilder.summary(event.getRenderedMessage() != null ? event.getRenderedMessage() : "");
    }

    return issueBuilder.build();
  }

  /**
   * Generates an issue code from the logging event.
   * If the event has an exception, generates a hash from the exception's stack trace.
   * Otherwise, generates a hash from the logger name and message.
   *
   * Issue codes are deterministic - same stack trace = same code.
   */
  private String getIssueCode(LoggingEvent event) {
    if (event.getThrowableInformation() != null) {
      Throwable throwable = event.getThrowableInformation().getThrowable();
      return getIssueCodeForException(throwable);
    } else {
      // For non-exception logs, use logger name + message
      String source = event.getLoggerName() + ":" + event.getMessage().toString();
      return AUTO_GENERATED_HASH_PREFIX +
          DigestUtils.sha256Hex(source).substring(0, AUTO_GENERATED_HASH_LENGTH).toUpperCase();
    }
  }

  /**
   * Generates an issue code from an exception's stack trace.
   * The stack trace is processed to exclude exception messages (which may vary)
   * but include class names and line numbers (which are stable).
   *
   * This ensures the same exception at the same location always gets the same code.
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
   * Converts Log4j Level to Gobblin IssueSeverity.
   */
  private IssueSeverity convertSeverity(Level level) {
    if (level == null) {
      return IssueSeverity.INFO;
    }

    // Map Log4j levels to IssueSeverity
    if (level.isGreaterOrEqual(Level.FATAL)) {
      return IssueSeverity.FATAL;
    } else if (level.isGreaterOrEqual(Level.ERROR)) {
      return IssueSeverity.ERROR;
    } else if (level.isGreaterOrEqual(Level.WARN)) {
      return IssueSeverity.WARN;
    } else if (level.isGreaterOrEqual(Level.INFO)) {
      return IssueSeverity.INFO;
    } else {
      return IssueSeverity.DEBUG;
    }
  }

  @Override
  public void close() {
    // No resources to clean up
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }
}
