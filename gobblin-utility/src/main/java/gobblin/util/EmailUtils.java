/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.mail.Email;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * A utility class for sending emails.
 *
 * @author ynli
 */
public class EmailUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmailUtils.class);

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
    email.setHostName(state.getProp(ConfigurationKeys.EMAIL_HOST_KEY, ConfigurationKeys.DEFAULT_EMAIL_HOST));
    if (state.contains(ConfigurationKeys.EMAIL_SMTP_PORT_KEY)) {
      email.setSmtpPort(state.getPropAsInt(ConfigurationKeys.EMAIL_SMTP_PORT_KEY));
    }
    email.setFrom(state.getProp(ConfigurationKeys.EMAIL_FROM_KEY));
    if (state.contains(ConfigurationKeys.EMAIL_USER_KEY) && state.contains(ConfigurationKeys.EMAIL_PASSWORD_KEY)) {
      email.setAuthentication(state.getProp(ConfigurationKeys.EMAIL_USER_KEY),
          state.getProp(ConfigurationKeys.EMAIL_PASSWORD_KEY));
    }
    Iterable<String> tos =
        Splitter.on(',').trimResults().omitEmptyStrings().split(state.getProp(ConfigurationKeys.EMAIL_TOS_KEY));
    for (String to : tos) {
      email.addTo(to);
    }

    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      LOGGER.error("Failed to get the host name", uhe);
      hostName = "unknown";
    }

    email.setSubject(subject);
    String fromHostLine = String.format("This email was sent from host: %s\n\n", hostName);
    email.setMsg(fromHostLine + message);
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
  public static void sendJobCompletionEmail(String jobName, String message, String state, State jobState)
      throws EmailException {

    sendEmail(jobState,
        String.format("Gobblin notification: most recent run of job %s has completed with state %s", jobName, state),
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
  public static void sendJobFailureAlertEmail(String jobName, String message, int failures, State jobState)
      throws EmailException {

    sendEmail(jobState, String
            .format("Gobblin alert: job %s has failed %d %s consecutively in the past", jobName, failures,
                failures > 1 ? "times" : "time"), message);
  }
}
