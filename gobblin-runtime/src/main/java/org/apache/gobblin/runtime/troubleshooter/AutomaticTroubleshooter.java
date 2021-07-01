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
public interface AutomaticTroubleshooter {

  /**
   * Sends the current collection of issues as GobblinTrackingEvents.
   *
   * Those events can be consumed by upstream and analytical systems.
   *
   * Can be disabled with
   * {@link org.apache.gobblin.configuration.ConfigurationKeys.TROUBLESHOOTER_DISABLE_EVENT_REPORTING}.
   * */
  void reportJobIssuesAsEvents(EventSubmitter eventSubmitter)
      throws TroubleshooterException;

  /**
   * This method will sort, filter and enhance the list of issues to make it more meaningful for the user.
   */
  void refineIssues()
      throws TroubleshooterException;

  /**
   * Logs a human-readable prioritized list of issues.
   *
   * The message will include the minimal important information about each issue.
   */
  void logIssueSummary()
      throws TroubleshooterException;

  /**
   * Logs a human-readable prioritized list of issues, including extra details.
   *
   * The message will include extended information about each issue, such as a stack trace and extra properties.
   */
  void logIssueDetails()
      throws TroubleshooterException;

  /**
   * Returns a human-readable prioritized list of issues as text.
   *
   * The message will include the minimal important information about the issue.
   *
   * @see #getIssueDetailsMessage()
   */
  String getIssueSummaryMessage()
      throws TroubleshooterException;

  /**
   * Returns a human-readable prioritized list of issues as text.
   *
   * The output includes additional information like stack traces, log sources and extra properties
   *
   * @see #getIssueSummaryMessage()
   */
  String getIssueDetailsMessage()
      throws TroubleshooterException;

  IssueRepository getIssueRepository();

  void start();

  void stop();
}
