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

package org.apache.gobblin.troubleshooter;

import java.util.List;
import java.util.Objects;

import org.apache.commons.text.TextStringBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterConfig;
import org.apache.gobblin.runtime.troubleshooter.Issue;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.IssueRefinery;
import org.apache.gobblin.runtime.troubleshooter.IssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;


/**
 * @see org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter
 * */
@Slf4j
@Singleton
public class AutomaticTroubleshooterImpl implements AutomaticTroubleshooter {
  private final AutomaticTroubleshooterConfig config;
  private final IssueRefinery issueRefinery;

  @Getter
  private final IssueRepository issueRepository;

  private AutoTroubleshooterLogAppender troubleshooterLogger;

  @Inject
  public AutomaticTroubleshooterImpl(AutomaticTroubleshooterConfig config, IssueRepository issueRepository,
      IssueRefinery issueRefinery) {
    this.config = Objects.requireNonNull(config);
    this.issueRepository = Objects.requireNonNull(issueRepository);
    this.issueRefinery = Objects.requireNonNull(issueRefinery);

    if (config.isDisabled()) {
      throw new RuntimeException("Cannot create a real troubleshooter because it is disabled in configuration. "
                                     + "Use AutomaticTroubleshooterFactory that will create "
                                     + "a NoopAutomaticTroubleshooter for this case.");
    }
  }

  @Override
  public void start() {
    setupLogAppender();
  }

  @Override
  public void stop() {
    removeLogAppender();
  }

  private void setupLogAppender() {
    org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();

    troubleshooterLogger = new AutoTroubleshooterLogAppender(issueRepository);
    troubleshooterLogger.setThreshold(Level.WARN);
    troubleshooterLogger.activateOptions();
    rootLogger.addAppender(troubleshooterLogger);

    log.info("Configured logger for automatic troubleshooting");
  }

  private void removeLogAppender() {
    org.apache.log4j.Logger rootLogger = LogManager.getRootLogger();
    rootLogger.removeAppender(troubleshooterLogger);
    log.info("Removed logger for automatic troubleshooting. Processed {} events.",
             troubleshooterLogger.getProcessedEventCount());
  }

  @Override
  public void reportJobIssuesAsEvents(EventSubmitter eventSubmitter)
      throws TroubleshooterException {
    if (config.isDisableEventReporting()) {
      log.info(
          "Troubleshooter will not report issues as GobblinTrackingEvents. Remove the following property to re-enable it: "
              + ConfigurationKeys.TROUBLESHOOTER_DISABLE_EVENT_REPORTING);
      return;
    }

    List<Issue> issues = issueRepository.getAll();
    log.info("Reporting troubleshooter issues as Gobblin tracking events. Issue count: " + issues.size());

    for (Issue issue : issues) {
      IssueEventBuilder eventBuilder = new IssueEventBuilder(IssueEventBuilder.JOB_ISSUE);
      eventBuilder.setIssue(issue);
      eventSubmitter.submit(eventBuilder);
    }
  }

  @Override
  public void refineIssues()
      throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();

    List<Issue> refinedIssues = issueRefinery.refine(ImmutableList.copyOf(issues));
    issueRepository.replaceAll(refinedIssues);
  }

  @Override
  public void logIssueSummary()
      throws TroubleshooterException {
    log.info(getIssueSummaryMessage());
  }

  @Override
  public void logIssueDetails()
      throws TroubleshooterException {
    log.info(getIssueDetailsMessage());
  }

  @Override
  public String getIssueSummaryMessage()
      throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();
    TextStringBuilder sb = new TextStringBuilder();
    sb.appendln("");
    sb.appendln("vvvvv============= Issues (summary) =============vvvvv");

    for (int i = 0; i < issues.size(); i++) {
      Issue issue = issues.get(i);

      sb.appendln("%s) %s %s %s | source: %s", i + 1, issue.getSeverity().toString(), issue.getCode(),
                  issue.getSummary(), issue.getSourceClass());
    }
    sb.append("^^^^^=============================================^^^^^");
    return sb.toString();
  }

  @Override
  public String getIssueDetailsMessage()
      throws TroubleshooterException {
    List<Issue> issues = issueRepository.getAll();

    TextStringBuilder sb = new TextStringBuilder();
    sb.appendln("");
    sb.appendln("vvvvv============= Issues (detailed) =============vvvvv");

    for (int i = 0; i < issues.size(); i++) {
      Issue issue = issues.get(i);

      sb.appendln("%s) %s %s %s", i + 1, issue.getSeverity().toString(), issue.getCode(), issue.getSummary());
      sb.appendln("\tsource: %s", issue.getSourceClass());

      if (issue.getDetails() != null) {
        sb.appendln("\t" + issue.getDetails().replaceAll(System.lineSeparator(), System.lineSeparator() + "\t"));
      }

      if (issue.getProperties() != null) {
        issue.getProperties().forEach((key, value) -> {
          sb.appendln("\t%s: %s", key, value);
        });
      }
    }
    sb.append("^^^^^================================================^^^^^");
    return sb.toString();
  }
}