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

import java.util.List;
import java.util.Objects;

import org.apache.commons.text.TextStringBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;


/**
 * Automatic troubleshooter will identify and prioritize the problems with the job, and display a summary to the user.
 *
 * Troubleshooter will collect errors & warnings from logs and combine them with various health checks. After that
 * you can {@link #refineIssues()} to prioritize them and filter out noise, and then {@link #logIssueSummary()}
 * to show a human-readable list of issues.
 *
 * Implementation and architecture notes:
 *
 * We convert log messages and health check results to {@link Issue}s. They will be shown to the user at the end of
 * the job log. To avoid overwhelming the user, we will only collect a fixed number of issues, and will de-duplicate
 * them, so that each type of problem is shown only once.
 *
 * Issues will be emitted in GobblinTrackingEvents at the end of the job, so that they can be collected by Gobblin
 * service, and used for future platform-wide analysis.
 *
 * */
@Slf4j
@Singleton
public class AutomaticTroubleshooter {
  private final AutomaticTroubleshooterConfig config;
  private final IssueRefinery issueRefinery;

  @Getter
  private final IssueRepository issueRepository;

  private AutoTroubleshooterLogAppender troubleshooterLogger;

  @Inject
  public AutomaticTroubleshooter(AutomaticTroubleshooterConfig config, IssueRepository issueRepository,
      IssueRefinery issueRefinery) {
    this.config = Objects.requireNonNull(config);
    this.issueRepository = Objects.requireNonNull(issueRepository);
    this.issueRefinery = Objects.requireNonNull(issueRefinery);
  }

  /**
   * Configures a troubleshooter that will be used inside Gobblin job or task.
   *
   * It will use small in-memory storage for issues.
   * */
  public static AutomaticTroubleshooter createForJob(Config config) {
    AutomaticTroubleshooterConfig troubleshooterConfig = new AutomaticTroubleshooterConfig(config);
    InMemoryIssueRepository issueRepository = new InMemoryIssueRepository();
    DefaultIssueRefinery issueRefinery = new DefaultIssueRefinery();
    return new AutomaticTroubleshooter(troubleshooterConfig, issueRepository, issueRefinery);
  }

  public void start() {
    if (config.isDisabled()) {
      logDisabledMessage();
      return;
    }
    setupLogAppender();
  }

  public void stop() {
    if (config.isDisabled()) {
      return;
    }
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

  /**
   * Sends the current collection of issues as GobblinTrackingEvents.
   *
   * Those events can be consumed by upstream and analytical systems.
   *
   * Can be disabled with {@link ConfigurationKeys.TROUBLESHOOTER_DISABLE_EVENT_REPORTING}.
   * */
  public void reportJobIssuesAsEvents(EventSubmitter eventSubmitter)
      throws TroubleshooterException {
    if (config.isDisabled()) {
      return;
    }
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

  /**
   * This method will sort, filter and enhance the list of issues to make it more meaningful for the user.
   */
  public void refineIssues()
      throws TroubleshooterException {

    if (config.isDisabled()) {
      return;
    }
    List<Issue> issues = issueRepository.getAll();

    List<Issue> refinedIssues = issueRefinery.refine(ImmutableList.copyOf(issues));
    issueRepository.replaceAll(refinedIssues);
  }

  /**
   * Logs a human-readable prioritized list of issues.
   *
   * The message will include the minimal important information about each issue.
   */
  public void logIssueSummary()
      throws TroubleshooterException {
    if (config.isDisabled()) {
      logDisabledMessage();
      return;
    }
    log.info(getIssueSummaryMessage());
  }

  /**
   * Logs a human-readable prioritized list of issues, including extra details.
   *
   * The message will include extended information about each issue, such as a stack trace and extra properties.
   */
  public void logIssueDetails()
      throws TroubleshooterException {
    if (config.isDisabled()) {
      logDisabledMessage();
      return;
    }
    log.info(getIssueDetailsMessage());
  }

  /**
   * Returns a human-readable prioritized list of issues as text.
   *
   * The message will include the minimal important information about the issue.
   *
   * @see #getIssueDetailsMessage()
   */
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

  /**
   * Returns a human-readable prioritized list of issues as text.
   *
   * The output includes additional information like stack traces, log sources and extra properties
   *
   * @see #getIssueSummaryMessage()
   */
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

  private void logDisabledMessage() {
    log.info("Troubleshooter is disabled. Remove the following property to re-enable it: "
                 + ConfigurationKeys.TROUBLESHOOTER_DISABLED);
  }
}