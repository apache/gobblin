package com.linkedin.uif.runtime;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.mail.EmailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.util.EmailUtils;

/**
 * An implementation of {@link JobListener} that sends a notification .
 */
public class EmailNotificationJobListener implements JobListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailNotificationJobListener.class);

    @Override
    public void jobCompleted(JobState jobState) {
        boolean emailNotificationEnabled = Boolean.valueOf(
                jobState.getProp(ConfigurationKeys.EMAIL_NOTIFICATION_ENABLED_KEY, "true"));
        if (!emailNotificationEnabled) {
            return;
        }

        try {
            // The email content is a json document converted from the job state
            StringWriter stringWriter = new StringWriter();
            JsonWriter jsonWriter = new JsonWriter(stringWriter);
            jsonWriter.setIndent("\t");
            jobState.toJson(jsonWriter);
            if (jobState.getState() == JobState.RunningState.FAILED) {
                EmailUtils.sendJobFailureAlertEmail(
                        jobState.getJobName(), stringWriter.toString(), jobState);
            } else {
                EmailUtils.sendJobCompletionEmail(
                        jobState.getJobName(), stringWriter.toString(), jobState);
            }
        } catch (IOException ioe) {
            LOGGER.error("Failed to convert job state to json document", ioe);
        } catch (EmailException ee) {
            LOGGER.error("Failed to send job completion notification email", ee);
        }
    }
}
