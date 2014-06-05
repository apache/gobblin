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
     * A general method for sending alert emails.
     *
     * @param state A {@link State} object containing configuration properties
     * @param subject Email subject
     * @param message Email message
     */
    public static void sendAlertEmail(State state, String subject, String message)
            throws EmailException {

        Email email = new SimpleEmail();
        email.setHostName(state.getProp(ConfigurationKeys.EMAIL_HOST_KEY,
                ConfigurationKeys.DEFAULT_EMAIL_HOST));
        email.setSmtpPort(state.getPropAsInt(ConfigurationKeys.EMAIL_SMTP_PORT_KEY,
                ConfigurationKeys.DEFAULT_EMAIL_SMTP_PORT));
        email.setFrom(state.getProp(ConfigurationKeys.EMAIL_FROM_KEY));
        email.setAuthentication(
                state.getProp(ConfigurationKeys.EMAIL_USER_KEY),
                state.getProp(ConfigurationKeys.EMAIL_PASSWORD_KEY));
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
     * Send a job failure alert email.
     *
     * @param jobName Job name
     * @param message Email message
     * @param jobState Job state
     */
    public static void sendJobFailureAlertEmail(String jobName, String message, State jobState)
            throws EmailException {

        int failures = jobState.getPropAsInt(ConfigurationKeys.JOB_FAILURES_KEY);
        sendAlertEmail(
                jobState,
                String.format(
                        "Gobblin Alert: Job %s has failed %d times consecutively in the past",
                        jobName, failures),
                message);
    }
}
