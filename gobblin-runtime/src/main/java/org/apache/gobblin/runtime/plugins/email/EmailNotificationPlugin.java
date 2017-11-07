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
package org.apache.gobblin.runtime.plugins.email;

import java.net.URI;

import org.apache.commons.mail.EmailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState.RunningState;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.GobblinInstancePlugin;
import org.apache.gobblin.runtime.api.GobblinInstancePluginFactory;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.api.JobExecutionState;
import org.apache.gobblin.runtime.api.JobLifecycleListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecSchedule;
import org.apache.gobblin.runtime.instance.StandardGobblinInstanceDriver;
import org.apache.gobblin.runtime.instance.plugin.BaseIdlePluginImpl;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.EmailUtils;

/**
 * A plugin that attaches an email notification listener to a {@link GobblinInstanceDriver}. The listener sends emails
 * on job completion
 */
public class EmailNotificationPlugin extends BaseIdlePluginImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotificationPlugin.class);

  /**
   * An instance level setting to disable email notifications to all job launched by an instance
   */
  public static final String EMAIL_NOTIFICATIONS_DISABLED_KEY = StandardGobblinInstanceDriver.INSTANCE_CFG_PREFIX
      + ".emailNotifications.disabled";
  public static final boolean EMAIL_NOTIFICATIONS_DISABLED_DEFAULT = false;

  public EmailNotificationPlugin(GobblinInstanceDriver instance) {
    super(instance);
  }

  @Override
  protected void startUp() throws Exception {
    instance.registerJobLifecycleListener(new EmailNotificationListerner());
    LOGGER.info("Started Email Notification Plugin");
  }

  public static class Factory implements GobblinInstancePluginFactory {

    @Override
    public GobblinInstancePlugin createPlugin(GobblinInstanceDriver instance) {
      return new EmailNotificationPlugin(instance);
    }
  }

  /**
   * Sends emails when job completes with FAILED, COMMITED or CANCELLED state.
   * Emails sent when job fails with FAILED status can be turned off by setting {@link ConfigurationKeys#ALERT_EMAIL_ENABLED_KEY} to false
   * Emails sent when job completes with COMMITTED/CANCELLED status can be turned off by
   * setting {@link ConfigurationKeys#NOTIFICATION_EMAIL_ENABLED_KEY} to false
   */
  private static class EmailNotificationListerner implements JobLifecycleListener {

    @Override
    public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {

      if (newStatus.isDone() && !previousStatus.isDone()) {
        boolean alertEmailEnabled =
            ConfigUtils.getBoolean(state.getJobSpec().getConfig(), ConfigurationKeys.ALERT_EMAIL_ENABLED_KEY, false);
        boolean notificationEmailEnabled =
            ConfigUtils.getBoolean(state.getJobSpec().getConfig(), ConfigurationKeys.NOTIFICATION_EMAIL_ENABLED_KEY,
                false);

        // Send failure emails
        if (alertEmailEnabled && newStatus.isFailure()) {
          try {
            LOGGER.info("Sending job failure email for job: {}", state.getJobSpec().toShortString());
            EmailUtils.sendJobFailureAlertEmail(state.getJobSpec().toShortString(),
                getEmailBody(state, previousStatus, newStatus), 1,
                ConfigUtils.configToState(state.getJobSpec().getConfig()));
          } catch (EmailException ee) {
            LOGGER.error("Failed to send job failure alert email for job " + state.getJobSpec().toShortString(), ee);
          }
          return;
        }

        // Send job completion emails
        if (notificationEmailEnabled && (newStatus.isCancelled() || newStatus.isSuccess())) {
          try {
            LOGGER.info("Sending job completion email for job: {}", state.getJobSpec().toShortString());
            EmailUtils.sendJobCompletionEmail(state.getJobSpec().toShortString(),
                getEmailBody(state, previousStatus, newStatus), newStatus.toString(),
                ConfigUtils.configToState(state.getJobSpec().getConfig()));
          } catch (EmailException ee) {
            LOGGER.error("Failed to send job completion notification email for job "
                + state.getJobSpec().toShortString(), ee);
          }
        }
      }
    }

    private static String getEmailBody(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
      return new StringBuilder().append("JobId: ")
          .append(state.getJobSpec().getConfig().getString(ConfigurationKeys.JOB_ID_KEY))
          .append("RunningState: ").append(newStatus.toString()).append("\n")
          .append("JobExecutionState: ").append(state.getJobSpec().toLongString()).append("\n")
          .append("ExecutionMetadata: ").append(state.getExecutionMetadata()).toString();
    }

    @Override
    public void onAddJob(JobSpec addedJob) {
    }

    @Override
    public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
    }

    @Override
    public void onUpdateJob(JobSpec updatedJob) {
    }

    @Override
    public void onJobScheduled(JobSpecSchedule jobSchedule) {
    }

    @Override
    public void onJobUnscheduled(JobSpecSchedule jobSchedule) {
    }

    @Override
    public void onJobTriggered(JobSpec jobSpec) {
    }

    @Override
    public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    }

    @Override
    public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    }

    @Override
    public void onJobLaunch(JobExecutionDriver jobDriver) {
    }

  }
}
