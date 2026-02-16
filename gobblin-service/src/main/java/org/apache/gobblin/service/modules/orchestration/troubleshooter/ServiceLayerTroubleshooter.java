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
import org.apache.log4j.LogManager;
import org.slf4j.MDC;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.InMemoryIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * Service-layer troubleshooter that captures exceptions and errors occurring in the GaaS
 * orchestration/service layer (before job submission to executors).
 *
 * <p>This is the service-layer equivalent of AutomaticTroubleshooterImpl.
 * It uses MDC (Mapped Diagnostic Context) for flow/job context extraction instead of JobState.
 * @see ServiceLayerLogAppender
 * @see org.apache.gobblin.service.modules.orchestration.proc.DagProc
 */
@Slf4j
public class ServiceLayerTroubleshooter {

  /**
   * Repository for storing captured issues during this execution.
   * Uses InMemoryIssueRepository since issues are submitted immediately after capture.
   */
  @Getter
  private final IssueRepository issueRepository;

  /**
   * Log appender that intercepts log.error()/log.warn() calls.
   */
  private ServiceLayerLogAppender logAppender;

  /**
   * If true, only capture logs when MDC has complete flow context.
   * If false, capture all logs regardless of MDC state.
   */
  private final boolean requireMdcContext;

  /**
   * Maximum number of issues to capture per execution to prevent memory issues.
   */
  private final int maxIssuesPerExecution;

  /**
   * Flag to track if troubleshooter is currently active.
   */
  private boolean isStarted = false;

  /**
   * Creates a new ServiceLayerTroubleshooter.
   *
   * @param requireMdcContext If true, only capture logs when MDC has flow context.
   *                          Set to true in production to ensure correct issue attribution.
   *                          Set to false for testing or debugging.
   * @param maxIssuesPerExecution Maximum number of issues to capture per execution
   */
  public ServiceLayerTroubleshooter(boolean requireMdcContext, int maxIssuesPerExecution) {
    this.issueRepository = new InMemoryIssueRepository();
    this.requireMdcContext = requireMdcContext;
    this.maxIssuesPerExecution = maxIssuesPerExecution;
  }

  /**
   * Starts capturing log events. Should be called after MDC context is set.
   *
   * <p>Registers a Log4j appender to the root logger that will intercept
   * WARN and ERROR level log events.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   *
   * @throws IllegalStateException if called when already started
   */
  public synchronized void start() {
    if (isStarted) {
      log.warn("ServiceLayerTroubleshooter.start() called when already started. Ignoring.");
      return;
    }

    org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();

    logAppender = new ServiceLayerLogAppender(issueRepository, requireMdcContext, maxIssuesPerExecution);
    logAppender.setThreshold(Level.WARN); // Capture WARN, ERROR, FATAL
    logAppender.activateOptions();

    rootLogger.addAppender(logAppender);
    isStarted = true;

    log.info("ServiceLayerTroubleshooter started for context: flowGroup={}, flowName={}, flowExecutionId={}, jobName={}, maxIssues={}",
        MDC.get(ConfigurationKeys.FLOW_GROUP_KEY),
        MDC.get(ConfigurationKeys.FLOW_NAME_KEY),
        MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY),
        MDC.get(ConfigurationKeys.JOB_NAME_KEY),
        maxIssuesPerExecution);
  }

  /**
   * Stops capturing log events and removes the appender.
   *
   * <p>This method is idempotent - calling it multiple times has no additional effect.
   *
   * <p><b>Important:</b> This does NOT clear the issue repository.
   * Call {@link #reportIssuesAsEvents(EventSubmitter)} to submit captured issues.
   */
  public synchronized void stop() {
    if (!isStarted) {
      log.debug("ServiceLayerTroubleshooter.stop() called when not started. Ignoring.");
      return;
    }

    if (logAppender != null) {
      org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
      rootLogger.removeAppender(logAppender);

      log.info("ServiceLayerTroubleshooter stopped. Captured {}/{} issues (processed {} events, skipped {} events).",
          logAppender.getCapturedIssueCount(), maxIssuesPerExecution,
          logAppender.getProcessedEventCount(), logAppender.getSkippedEventCount());

      logAppender = null;
    }

    isStarted = false;
  }

  /**
   * Submits all captured issues as IssueEvents with proper flow/job context metadata.
   *
   * <p>Issues are submitted via the provided EventSubmitter, which sends them to the
   * tracking event stream where JobIssueEventHandler will process them and store in
   * MultiContextIssueRepository.
   *
   * <p>This method extracts flow context from MDC and adds it as metadata to each issue event,
   * ensuring issues are correctly associated with their originating flow/job.
   *
   * @param eventSubmitter EventSubmitter for publishing IssueEvents
   * @throws TroubleshooterException if unable to retrieve issues from repository
   */
  public void reportIssuesAsEvents(EventSubmitter eventSubmitter) throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();

    if (issues.isEmpty()) {
      log.info("ServiceLayerTroubleshooter: No issues to report.");
      return;
    }

    log.info("ServiceLayerTroubleshooter: Reporting {} service-layer issues as tracking events", issues.size());

    // Extract context from MDC
    String flowGroup = MDC.get(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = MDC.get(ConfigurationKeys.FLOW_NAME_KEY);
    String flowExecutionId = MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
    String jobName = MDC.get(ConfigurationKeys.JOB_NAME_KEY);

    // Validate MDC context is available
    if (flowGroup == null || flowName == null || flowExecutionId == null) {
      log.warn("ServiceLayerTroubleshooter: MDC context missing when reporting issues. " +
          "flowGroup={}, flowName={}, flowExecutionId={}. Issues may not be correctly attributed.",
          flowGroup, flowName, flowExecutionId);
      // Continue anyway - better to have issues with incomplete context than none
    }

    int successCount = 0;
    int failureCount = 0;

    for (Issue issue : issues) {
      try {
        IssueEventBuilder eventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);
        eventBuilder.setIssue(issue);

        // Add flow/job context metadata for JobIssueEventHandler to extract context ID
        if (flowGroup != null) {
          eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
        }
        if (flowName != null) {
          eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
        }
        if (flowExecutionId != null) {
          eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, flowExecutionId);
        }
        if (jobName != null) {
          eventBuilder.addMetadata(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
        }

        // Add metadata to indicate this is a service-layer issue
        eventBuilder.addMetadata("issueSource", "service-layer");

        eventSubmitter.submit(eventBuilder);
        successCount++;

      } catch (Exception e) {
        failureCount++;
        log.error("ServiceLayerTroubleshooter: Failed to submit issue event. " +
            "Issue code: {}, summary: {}", issue.getCode(), issue.getSummary(), e);
      }
    }

    log.info("ServiceLayerTroubleshooter: Issue event submission complete. " +
        "Success: {}, Failures: {}", successCount, failureCount);
  }

  /**
   * Logs a summary of captured issues for debugging.
   *
   * <p>Formats issues in a human-readable format showing severity, summary, and code.
   *
   * @throws TroubleshooterException if unable to retrieve issues from repository
   */
  public void logIssueSummary() throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();

    if (issues.isEmpty()) {
      log.debug("ServiceLayerTroubleshooter: No issues captured.");
      return;
    }

    StringBuilder summary = new StringBuilder();
    summary.append("\n========= Service Layer Issues Summary =========\n");
    summary.append("Flow Context: flowGroup=").append(MDC.get(ConfigurationKeys.FLOW_GROUP_KEY))
        .append(", flowName=").append(MDC.get(ConfigurationKeys.FLOW_NAME_KEY))
        .append(", flowExecutionId=").append(MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY))
        .append(", jobName=").append(MDC.get(ConfigurationKeys.JOB_NAME_KEY))
        .append("\n");
    summary.append("Total Issues: ").append(issues.size()).append("\n");
    summary.append("------------------------------------------------\n");

    for (int i = 0; i < issues.size(); i++) {
      Issue issue = issues.get(i);
      summary.append(String.format("%d. [%s] %s (code: %s)\n",
          i + 1, issue.getSeverity(), issue.getSummary(), issue.getCode()));

      // For ERROR/FATAL, include first line of details
      if (issue.getSeverity() == org.apache.gobblin.runtime.troubleshooter.IssueSeverity.ERROR
          || issue.getSeverity() == org.apache.gobblin.runtime.troubleshooter.IssueSeverity.FATAL) {
        String details = issue.getDetails();
        if (details != null && !details.isEmpty()) {
          String firstLine = details.split("\n")[0];
          summary.append("   Details: ").append(firstLine).append("\n");
        }
      }
    }

    summary.append("================================================\n");
    log.info(summary.toString());
  }

  /**
   * Returns the number of issues currently captured in the repository.
   */
  public int getIssueCount() {
    try {
      return issueRepository.getAll().size();
    } catch (TroubleshooterException e) {
      log.warn("Failed to get issue count from repository", e);
      return 0;
    }
  }

  /**
   * Returns true if the troubleshooter is currently active (started but not stopped).
   */
  public boolean isStarted() {
    return isStarted;
  }

  /**
   * Clears all captured issues from the repository.
   *
   * <p>This is useful for testing or when reusing a troubleshooter instance.
   * In production, each DagProc execution creates a new troubleshooter instance.
   */
  public void clear() {
    try {
      issueRepository.removeAll();
    } catch (TroubleshooterException e) {
      log.warn("Failed to clear issue repository", e);
    }
  }
}
