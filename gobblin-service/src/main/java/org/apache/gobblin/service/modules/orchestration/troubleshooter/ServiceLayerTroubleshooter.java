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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
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
 * Captures WARN and ERROR log events from the service/orchestration layer and converts them
 * to troubleshooting Issues. Uses MDC for flow context extraction.
 **
 * @see ServiceLayerLog4j2Appender
 * @see org.apache.gobblin.service.modules.orchestration.proc.DagProc
 */
@Slf4j
public class ServiceLayerTroubleshooter {

  @Getter
  private final IssueRepository issueRepository;

  private ServiceLayerLog4j2Appender logAppender;
  private final boolean requireMdcContext;
  private final int maxIssuesPerExecution;
  private boolean isStarted = false;

  /**
   * @param requireMdcContext If true, only capture events with valid MDC flow context
   * @param maxIssuesPerExecution Maximum issues to capture per execution
   */
  public ServiceLayerTroubleshooter(boolean requireMdcContext, int maxIssuesPerExecution) {
    this.issueRepository = new InMemoryIssueRepository();
    this.requireMdcContext = requireMdcContext;
    this.maxIssuesPerExecution = maxIssuesPerExecution;
  }

  /**
   * Starts capturing log events. Should be called after MDC context is set.
   *
   * <p>Registers a Log4j 2.x appender to intercept WARN and ERROR level events.
   * The appender filters events to only capture those from the current flow execution.
   */
  public synchronized void start() {
    if (isStarted) {
      log.warn("ServiceLayerTroubleshooter already started. Ignoring duplicate start() call.");
      return;
    }

    Logger rootLogger = (Logger) LogManager.getRootLogger();

    // Extract flow context for event filtering
    String flowGroup = MDC.get(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = MDC.get(ConfigurationKeys.FLOW_NAME_KEY);
    String flowExecutionId = MDC.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);

    logAppender = new ServiceLayerLog4j2Appender(
        "ServiceLayerTroubleshooterAppender",
        null,
        issueRepository,
        requireMdcContext,
        maxIssuesPerExecution,
        flowGroup,
        flowName,
        flowExecutionId
    );

    logAppender.start();
    rootLogger.addAppender(logAppender);
    isStarted = true;

    log.debug("ServiceLayerTroubleshooter started for flowGroup={}, flowName={}, flowExecutionId={}, maxIssues={}",
        flowGroup, flowName, flowExecutionId, maxIssuesPerExecution);
  }

  /**
   * Stops capturing log events and removes the appender.
   * Does not clear the issue repository - call {@link #reportIssuesAsEvents} to submit issues.
   */
  public synchronized void stop() {
    if (!isStarted) {
      return;
    }

    if (logAppender != null) {
      Logger rootLogger = (Logger) LogManager.getRootLogger();
      rootLogger.removeAppender(logAppender);
      logAppender.stop();

      log.debug("ServiceLayerTroubleshooter stopped. Captured {}/{} issues (processed {} events, skipped {} events).",
          logAppender.getCapturedIssueCount(), maxIssuesPerExecution,
          logAppender.getProcessedEventCount(), logAppender.getSkippedEventCount());

      logAppender = null;
    }

    isStarted = false;
  }

  /**
   * Submits captured issues as tracking events with flow/job context metadata.
   * Issues are sent to the tracking stream where JobIssueEventHandler stores them.
   *
   * @param eventSubmitter EventSubmitter for publishing IssueEvents
   * @throws TroubleshooterException if unable to retrieve issues
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
    }

    int successCount = 0;
    int failureCount = 0;

    for (Issue issue : issues) {
      try {
        IssueEventBuilder eventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);
        eventBuilder.setIssue(issue);

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
   * Logs a summary of captured issues in human-readable format.
   */
  public void logIssueSummary() throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();

    if (issues.isEmpty()) {
      log.info("ServiceLayerTroubleshooter: No issues captured.");
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

  public int getIssueCount() {
    try {
      return issueRepository.getAll().size();
    } catch (TroubleshooterException e) {
      log.warn("Failed to get issue count from repository", e);
      return 0;
    }
  }

  public boolean isStarted() {
    return isStarted;
  }

  public void clear() {
    try {
      issueRepository.removeAll();
    } catch (TroubleshooterException e) {
      log.warn("Failed to clear issue repository", e);
    }
  }
}
