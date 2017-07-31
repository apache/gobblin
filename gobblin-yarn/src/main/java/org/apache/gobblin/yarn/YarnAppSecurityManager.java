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

package org.apache.gobblin.yarn;

import org.apache.gobblin.cluster.GobblinHelixMessagingService;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;

import com.typesafe.config.Config;

import org.apache.gobblin.util.ExecutorsUtils;


/**
 * A class for managing Kerberos login and token renewing on the client side that has access to
 * the keytab file.
 *
 * <p>
 *   This class works with {@link YarnContainerSecurityManager} to manage renewing of delegation
 *   tokens across the application. This class is responsible for login through a Kerberos keytab,
 *   renewing the delegation token, and storing the token to a token file on HDFS. It sends a
 *   Helix message to the controller and all the participants upon writing the token to the token
 *   file, which rely on the {@link YarnContainerSecurityManager} to read the token in the file
 *   upon receiving the message.
 * </p>
 *
 * <p>
 *   This class uses a scheduled task to do Kerberos re-login to renew the Kerberos ticket on a
 *   configurable schedule if login is from a keytab file. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewing interval are configurable.
 * </p>
 *
 * @author Yinan Li
 */
public class YarnAppSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnAppSecurityManager.class);

  private final Config config;

  private final HelixManager helixManager;
  private final FileSystem fs;
  private final Path tokenFilePath;
  private UserGroupInformation loginUser;
  private Token<? extends TokenIdentifier> token;

  private final long loginIntervalInMinutes;
  private final long tokenRenewIntervalInMinutes;

  private final ScheduledExecutorService loginExecutor;
  private final ScheduledExecutorService tokenRenewExecutor;
  private Optional<ScheduledFuture<?>> scheduledTokenRenewTask = Optional.absent();

  // This flag is used to tell if this is the first login. If yes, no token updated message will be
  // sent to the controller and the participants as they may not be up running yet. The first login
  // happens after this class starts up so the token gets regularly refreshed before the next login.
  private volatile boolean firstLogin = true;

  public YarnAppSecurityManager(Config config, HelixManager helixManager, FileSystem fs, Path tokenFilePath)
      throws IOException {
    this.config = config;
    this.helixManager = helixManager;
    this.fs = fs;

    this.tokenFilePath = tokenFilePath;
    this.fs.makeQualified(tokenFilePath);
    this.loginUser = UserGroupInformation.getLoginUser();
    this.loginIntervalInMinutes = config.getLong(GobblinYarnConfigurationKeys.LOGIN_INTERVAL_IN_MINUTES);
    this.tokenRenewIntervalInMinutes = config.getLong(GobblinYarnConfigurationKeys.TOKEN_RENEW_INTERVAL_IN_MINUTES);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("KeytabReLoginExecutor")));
    this.tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("TokenRenewExecutor")));
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + YarnAppSecurityManager.class.getSimpleName());

    LOGGER.info(
        String.format("Scheduling the login task with an interval of %d minute(s)", this.loginIntervalInMinutes));

    // Schedule the Kerberos re-login task
    this.loginExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          // Cancel the currently scheduled token renew task
          if (scheduledTokenRenewTask.isPresent() && scheduledTokenRenewTask.get().cancel(true)) {
            LOGGER.info("Cancelled the token renew task");
          }

          loginFromKeytab();
          if (firstLogin) {
            firstLogin = false;
          }

          // Re-schedule the token renew task after re-login
          scheduleTokenRenewTask();
        } catch (IOException ioe) {
          LOGGER.error("Failed to login from keytab", ioe);
          throw Throwables.propagate(ioe);
        }
      }
    }, 0, this.loginIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Stopping the " + YarnAppSecurityManager.class.getSimpleName());

    if (this.scheduledTokenRenewTask.isPresent()) {
      this.scheduledTokenRenewTask.get().cancel(true);
    }
    ExecutorsUtils.shutdownExecutorService(this.loginExecutor, Optional.of(LOGGER));
    ExecutorsUtils.shutdownExecutorService(this.tokenRenewExecutor, Optional.of(LOGGER));
  }

  private void scheduleTokenRenewTask() {
    LOGGER.info(String.format("Scheduling the token renew task with an interval of %d minute(s)",
        this.tokenRenewIntervalInMinutes));

    this.scheduledTokenRenewTask = Optional.<ScheduledFuture<?>>of(
        this.tokenRenewExecutor.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            try {
              renewDelegationToken();
            } catch (IOException ioe) {
              LOGGER.error("Failed to renew delegation token", ioe);
              throw Throwables.propagate(ioe);
            } catch (InterruptedException ie) {
              LOGGER.error("Token renew task has been interrupted");
              Thread.currentThread().interrupt();
            }
          }
        }, this.tokenRenewIntervalInMinutes, this.tokenRenewIntervalInMinutes, TimeUnit.MINUTES));
  }

  /**
   * Renew the existing delegation token.
   */
  private synchronized void renewDelegationToken() throws IOException, InterruptedException {
    this.token.renew(this.fs.getConf());
    writeDelegationTokenToFile();

    if (!this.firstLogin) {
      // Send a message to the controller and all the participants if this is not the first login
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
      sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
    }
  }

  /**
   * Get a new delegation token for the current logged-in user.
   */
  @VisibleForTesting
  synchronized void getNewDelegationTokenForLoginUser() throws IOException {
    this.token = this.fs.getDelegationToken(this.loginUser.getShortUserName());
  }

  /**
   * Login the user from a given keytab file.
   */
  private void loginFromKeytab() throws IOException {
    String keyTabFilePath = this.config.getString(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH);
    if (Strings.isNullOrEmpty(keyTabFilePath)) {
      throw new IOException("Keytab file path is not defined for Kerberos login");
    }

    if (!new File(keyTabFilePath).exists()) {
      throw new IOException("Keytab file not found at: " + keyTabFilePath);
    }

    String principal = this.config.getString(GobblinYarnConfigurationKeys.KEYTAB_PRINCIPAL_NAME);
    if (Strings.isNullOrEmpty(principal)) {
      principal = this.loginUser.getShortUserName() + "/localhost@LOCALHOST";
    }

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication",
        UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase());
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);
    LOGGER.info(String.format("Logged in from keytab file %s using principal %s", keyTabFilePath, principal));

    this.loginUser = UserGroupInformation.getLoginUser();

    getNewDelegationTokenForLoginUser();
    writeDelegationTokenToFile();

    if (!this.firstLogin) {
      // Send a message to the controller and all the participants
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
      sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
    }
  }

  /**
   * Write the current delegation token to the token file.
   */
  @VisibleForTesting
  synchronized void writeDelegationTokenToFile() throws IOException {
    if (this.fs.exists(this.tokenFilePath)) {
      LOGGER.info("Deleting existing token file " + this.tokenFilePath);
      this.fs.delete(this.tokenFilePath, false);
    }

    LOGGER.info("Writing new or renewed token to token file " + this.tokenFilePath);
    YarnHelixUtils.writeTokenToFile(this.token, this.tokenFilePath, this.fs.getConf());
    // Only grand access to the token file to the login user
    this.fs.setPermission(this.tokenFilePath, new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));
  }

  @VisibleForTesting
  void sendTokenFileUpdatedMessage(InstanceType instanceType) {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(instanceType);
    /**
     * #HELIX-0.6.7-WORKAROUND
     * Add back when LIVESTANCES messaging is ported to 0.6 branch
    if (instanceType == InstanceType.PARTICIPANT) {
      criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
    }
     **/
    criteria.setSessionSpecific(true);

    Message tokenFileUpdatedMessage = new Message(Message.MessageType.USER_DEFINE_MSG,
        HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString().toLowerCase() + UUID.randomUUID().toString());
    tokenFileUpdatedMessage.setMsgSubType(HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());
    tokenFileUpdatedMessage.setMsgState(Message.MessageState.NEW);
    if (instanceType == InstanceType.CONTROLLER) {
      tokenFileUpdatedMessage.setTgtSessionId("*");
    }

    // #HELIX-0.6.7-WORKAROUND
    // Temporarily bypass the default messaging service to allow upgrade to 0.6.7 which is missing support
    // for messaging to instances
    //int messagesSent = this.helixManager.getMessagingService().send(criteria, tokenFileUpdatedMessage);
    GobblinHelixMessagingService messagingService = new GobblinHelixMessagingService(this.helixManager);

    int messagesSent = messagingService.send(criteria, tokenFileUpdatedMessage);
    LOGGER.info(String.format("Sent %d token file updated message(s) to the %s", messagesSent, instanceType));
  }
}
