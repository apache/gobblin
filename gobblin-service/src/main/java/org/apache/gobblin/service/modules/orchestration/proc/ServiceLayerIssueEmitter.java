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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;

import org.apache.commons.codec.digest.DigestUtils;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueSeverity;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;


/**
 * Utility for emitting orchestration/service-layer issues as {@link IssueEventBuilder} events.
 *
 * <p>Issues emitted here flow through the existing pipeline:
 * {@link IssueEventBuilder} → Kafka → {@link org.apache.gobblin.runtime.troubleshooter.JobIssueEventHandler}
 * → {@link org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository}
 *
 * <p>Issue codes are auto-generated with {@code S} prefix + 6-char hex hash, consistent with the
 * previous {@code ServiceLayerLog4j2Appender} convention. Executor-side issues use {@code T} prefix.
 *
 * <p>Thread safety: Each call creates its own {@link Issue} and {@link IssueEventBuilder} objects
 * on the stack. No shared mutable state. The {@link EventSubmitter} is thread-safe.
 * Flow/job context is passed explicitly (not read from MDC), preventing cross-flow contamination.
 */
@Slf4j
public final class ServiceLayerIssueEmitter {

  private static final int HASH_LENGTH = 6;
  private static final String HASH_PREFIX = "S";

  private ServiceLayerIssueEmitter() {
  }

  /**
   * Emit a flow-level issue (jobName = "NA"). Use for errors where no job exists yet
   * (e.g., compilation failures) or errors affecting the entire flow (e.g., flow SLA exceeded).
   */
  public static void emitFlowIssue(EventSubmitter eventSubmitter, Dag.DagId dagId,
      IssueSeverity severity, String summary) {
    emit(eventSubmitter, dagId.getFlowGroup(), dagId.getFlowName(),
        String.valueOf(dagId.getFlowExecutionId()), JobStatusRetriever.NA_KEY,
        severity, summary, "");
  }

  /**
   * Emit a flow-level issue with details.
   */
  public static void emitFlowIssue(EventSubmitter eventSubmitter, Dag.DagId dagId,
      IssueSeverity severity, String summary, String details) {
    emit(eventSubmitter, dagId.getFlowGroup(), dagId.getFlowName(),
        String.valueOf(dagId.getFlowExecutionId()), JobStatusRetriever.NA_KEY,
        severity, summary, details);
  }

  /**
   * Emit a flow-level issue using string identifiers (for callers without a DagId, e.g. FlowCompilationValidationHelper).
   */
  public static void emitFlowIssue(EventSubmitter eventSubmitter, String flowGroup, String flowName,
      String flowExecutionId, IssueSeverity severity, String summary) {
    emit(eventSubmitter, flowGroup, flowName, flowExecutionId, JobStatusRetriever.NA_KEY,
        severity, summary, "");
  }

  /**
   * Emit a job-level issue for a specific job. Use for errors tied to a particular job
   * (e.g., job submission failure, job start deadline exceeded).
   */
  public static void emitJobIssue(EventSubmitter eventSubmitter, Dag.DagId dagId, String jobName,
      IssueSeverity severity, String summary) {
    emit(eventSubmitter, dagId.getFlowGroup(), dagId.getFlowName(),
        String.valueOf(dagId.getFlowExecutionId()), jobName,
        severity, summary, "");
  }

  /**
   * Emit a job-level issue with details.
   */
  public static void emitJobIssue(EventSubmitter eventSubmitter, Dag.DagId dagId, String jobName,
      IssueSeverity severity, String summary, String details) {
    emit(eventSubmitter, dagId.getFlowGroup(), dagId.getFlowName(),
        String.valueOf(dagId.getFlowExecutionId()), jobName,
        severity, summary, details);
  }

  /**
   * Generates an issue code using S prefix + 6-char hex hash, consistent with the previous
   * ServiceLayerLog4j2Appender convention. The hash is based on the summary to enable deduplication.
   */
  static String generateIssueCode(String summary) {
    return HASH_PREFIX + DigestUtils.sha256Hex(summary).substring(0, HASH_LENGTH).toUpperCase();
  }

  private static void emit(EventSubmitter eventSubmitter, String flowGroup, String flowName,
      String flowExecutionId, String jobName, IssueSeverity severity,
      String summary, String details) {
    try {
      Issue issue = Issue.builder()
          .time(ZonedDateTime.now(ZoneOffset.UTC))
          .severity(severity)
          .code(generateIssueCode(summary))
          .summary(summary)
          .details(details != null ? details : "")
          .sourceClass(ServiceLayerIssueEmitter.class.getName())
          .properties(Collections.emptyMap())
          .build();

      IssueEventBuilder eventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);
      eventBuilder.setIssue(issue);
      eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, flowGroup);
      eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, flowName);
      eventBuilder.addMetadata(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, flowExecutionId);
      eventBuilder.addMetadata(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
      eventBuilder.addMetadata("issueSource", "service-layer");

      eventSubmitter.submit(eventBuilder);

      log.debug("Emitted service-layer issue: code={}, severity={}, jobName={}, summary={}",
          issue.getCode(), severity, jobName, summary);
    } catch (Exception e) {
      log.error("Failed to emit service-layer issue: summary={}", summary, e);
    }
  }
}
