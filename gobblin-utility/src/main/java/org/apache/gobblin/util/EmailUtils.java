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
import gobblin.password.PasswordManager;


/**
 * A utility class for sending emails.
 *
 * @author Yinan Li
 */
public class EmailUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmailUtils.class);

  /**
   * A general method for sending emails.
   *
   * @param state a {@link State} object containing configuration properties
   * @param subject email subject
   * @param message email message
   * @throws EmailException if there is anything wrong sending the email
   */
  public static void sendEmail(State state, String subject, String message) throws EmailException {
    Email email = new SimpleEmail();
    email.setHostName(state.getProp(ConfigurationKeys.EMAIL_HOST_KEY, ConfigurationKeys.DEFAULT_EMAIL_HOST));
    if (state.contains(ConfigurationKeys.EMAIL_SMTP_PORT_KEY)) {
      email.setSmtpPort(state.getPropAsInt(ConfigurationKeys.EMAIL_SMTP_PORT_KEY));
    }
    email.setFrom(state.getProp(ConfigurationKeys.EMAIL_FROM_KEY));
    if (state.contains(ConfigurationKeys.EMAIL_USER_KEY) && state.contains(ConfigurationKeys.EMAIL_PASSWORD_KEY)) {
      email.setAuthentication(state.getProp(ConfigurationKeys.EMAIL_USER_KEY),
          PasswordManager.getInstance(state).readPassword(state.getProp(ConfigurationKeys.EMAIL_PASSWORD_KEY)));
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
    String fromHostLine = String.format("This email was sent from host: %s%n%n", hostName);
    email.setMsg(fromHostLine + message);
    email.send();
  }

  /**
   * Send a job completion notification email.
   *
   * @param jobId job name
   * @param message email message
   * @param state job state
   * @param jobState a {@link State} object carrying job configuration properties
   * @throws EmailException if there is anything wrong sending the email
   */
  public static void sendJobCompletionEmail(String jobId, String message, String state, State jobState)
      throws EmailException {
    sendEmail(jobState, String.format("Gobblin notification: job %s has completed with state %s", jobId, state),
        message);
  }

  /**
   * Send a job cancellation notification email.
   *
   * @param jobId job name
   * @param message email message
   * @param jobState a {@link State} object carrying job configuration properties
   * @throws EmailException if there is anything wrong sending the email
   */
  public static void sendJobCancellationEmail(String jobId, String message, State jobState) throws EmailException {
    sendEmail(jobState, String.format("Gobblin notification: job %s has been cancelled", jobId), message);
  }

  /**
   * Send a job failure alert email.
   *
   * @param jobName job name
   * @param message email message
   * @param failures number of consecutive job failures
   * @param jobState a {@link State} object carrying job configuration properties
   * @throws EmailException if there is anything wrong sending the email
   */
  public static void sendJobFailureAlertEmail(String jobName, String message, int failures, State jobState)
      throws EmailException {
    sendEmail(jobState, String.format("Gobblin alert: job %s has failed %d %s consecutively in the past", jobName,
        failures, failures > 1 ? "times" : "time"), message);
  }
}
