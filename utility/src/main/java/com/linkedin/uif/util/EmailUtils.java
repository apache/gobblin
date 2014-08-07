package com.linkedin.uif.util;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

import com.google.common.base.Splitter;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;

/**
 * A utility class for sending emails.
 *
 * @author ynli
 */
public class EmailUtils {

    /**
     * A general method for sending emails.
     *
     * @param state A {@link State} object containing configuration properties
     * @param subject Email subject
     * @param message Email message
     */
    public static void sendEmail(State state, String subject, String message)
            throws EmailException {

        Email email = new SimpleEmail();
        email.setHostName(state.getProp(ConfigurationKeys.EMAIL_HOST_KEY,
                ConfigurationKeys.DEFAULT_EMAIL_HOST));
        if (state.contains(ConfigurationKeys.EMAIL_SMTP_PORT_KEY)) {
            email.setSmtpPort(state.getPropAsInt(ConfigurationKeys.EMAIL_SMTP_PORT_KEY));
        }
        email.setFrom(state.getProp(ConfigurationKeys.EMAIL_FROM_KEY));
        if (state.contains(ConfigurationKeys.EMAIL_USER_KEY)
                && state.contains(ConfigurationKeys.EMAIL_PASSWORD_KEY)) {
            email.setAuthentication(state.getProp(ConfigurationKeys.EMAIL_USER_KEY),
                                    state.getProp(ConfigurationKeys.EMAIL_PASSWORD_KEY));
        }
        Iterable<String> tos = Splitter.on(',').trimResults().omitEmptyStrings()
                .split(state.getProp(ConfigurationKeys.EMAIL_TOS_KEY));
        for (String to : tos) {
            email.addTo(to);
        }
        email.setSubject(subject);
        email.setMsg(message);
        email.send();
    }

    /**
     * Send a job completion notification email.
     *
     * @param jobName Job name
     * @param message Email message
     * @param state Job state
     * @param jobState A {@link State} object carrying job configuration properties
     * @throws EmailException
     */
    public static void sendJobCompletionEmail(String jobName, String message,
                                              String state, State jobState)
            throws EmailException {

        sendEmail(
                jobState,
                String.format(
                        "Gobblin notification: most recent run of job %s has completed with state %s",
                        jobName, state),
                message);
    }

    /**
     * Send a job failure alert email.
     *
     * @param jobName Job name
     * @param message Email message
     * @param failures Number of consecutive job failures
     * @param jobState A {@link State} object carrying job configuration properties
     */
    public static void sendJobFailureAlertEmail(String jobName, String message,
                                                int failures, State jobState)
            throws EmailException {

        sendEmail(
                jobState,
                String.format(
                        "Gobblin alert: job %s has failed %d %s consecutively in the past",
                        jobName, failures, failures > 1 ? "times" : "time"),
                message);
    }
}
