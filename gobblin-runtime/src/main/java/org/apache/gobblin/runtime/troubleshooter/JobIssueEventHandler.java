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

import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.util.GsonUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 *  This class will forward issues received from job events to shared repository.
 *
 *  It will additionally log received issues, so that they can be processed by an analytical systems to determine
 *  the overall platform health.
 * */
@Slf4j
public class JobIssueEventHandler {

  public static final String CONFIG_PREFIX = "gobblin.troubleshooter.jobIssueEventHandler.";
  public static final String LOG_RECEIVED_EVENTS = CONFIG_PREFIX + "logReceivedEvents";

  private static final Logger issueLogger =
      LoggerFactory.getLogger("org.apache.gobblin.runtime.troubleshooter.JobIssueLogger");

  private final MultiContextIssueRepository issueRepository;
  private final boolean logReceivedEvents;

  @Inject
  public JobIssueEventHandler(MultiContextIssueRepository issueRepository, Config config) {
    this(issueRepository, ConfigUtils.getBoolean(config, LOG_RECEIVED_EVENTS, true));
  }

  public JobIssueEventHandler(MultiContextIssueRepository issueRepository, boolean logReceivedEvents) {
    this.issueRepository = Objects.requireNonNull(issueRepository);
    this.logReceivedEvents = logReceivedEvents;
  }

  public void processEvent(GobblinTrackingEvent event) {
    if (!IssueEventBuilder.isIssueEvent(event)) {
      return;
    }

    String contextId;
    try {
      Properties metadataProperties = new Properties();
      metadataProperties.putAll(event.getMetadata());
      contextId = TroubleshooterUtils.getContextIdForJob(metadataProperties);
    } catch (Exception ex) {
      log.warn("Failed to extract context id from Gobblin tracking event with timestamp " + event.getTimestamp(), ex);
      return;
    }

    IssueEventBuilder issueEvent = IssueEventBuilder.fromEvent(event);

    try {
      issueRepository.put(contextId, issueEvent.getIssue());
    } catch (TroubleshooterException e) {
      log.warn(String.format("Failed to save issue to repository. Issue time: %s, code: %s",
                             issueEvent.getIssue().getTime(), issueEvent.getIssue().getCode()), e);
    }

    if (logReceivedEvents) {
      logEvent(issueEvent);
    }
  }

  private void logEvent(IssueEventBuilder issueEvent) {
    ImmutableMap<String, String> metadata = issueEvent.getMetadata();

    JobIssueLogEntry logEntry = new JobIssueLogEntry();
    logEntry.issue = issueEvent.getIssue();

    logEntry.flowGroup = metadata.getOrDefault(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, null);
    logEntry.flowName = metadata.getOrDefault(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, null);
    logEntry.flowExecutionId = metadata.getOrDefault(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, null);
    logEntry.jobName = metadata.getOrDefault(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, null);

    String serializedIssueEvent = GsonUtils.GSON_WITH_DATE_HANDLING.toJson(logEntry);
    issueLogger.info(serializedIssueEvent);
  }

  private static class JobIssueLogEntry {
    String flowName;
    String flowGroup;
    String flowExecutionId;
    String jobName;
    Issue issue;
  }
}
