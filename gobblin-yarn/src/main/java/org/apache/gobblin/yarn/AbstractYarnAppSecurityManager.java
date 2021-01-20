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

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
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

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinHelixMessagingService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * <p>
 *   The super class for key management
 *   This class uses a scheduled task to do re-login to re-fetch token on a
 *   configurable schedule. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewing interval are configurable.
 * </p>
 * @author Zihan Li
 */
public abstract class AbstractYarnAppSecurityManager extends AbstractIdleService {

  protected Logger LOGGER = LoggerFactory.getLogger(this.getClass().getName());

  protected Config config;

  protected final HelixManager helixManager;
  protected final FileSystem fs;
  protected final Path tokenFilePath;

  protected Credentials credentials = new Credentials();
  private final long loginIntervalInMinutes;
  private final long tokenRenewIntervalInMinutes;
  private final boolean isHelixClusterManaged;
  private final String helixInstanceName;
  private final ScheduledExecutorService loginExecutor;
  private final ScheduledExecutorService tokenRenewExecutor;

  private Optional<ScheduledFuture<?>> scheduledTokenRenewTask = Optional.absent();

  // This flag is used to tell if this is the first login. If yes, no token updated message will be
  // sent to the controller and the participants as they may not be up running yet. The first login
  // happens after this class starts up so the token gets regularly refreshed before the next login.
  protected volatile boolean firstLogin = true;

  public AbstractYarnAppSecurityManager(Config config, HelixManager helixManager, FileSystem fs, Path tokenFilePath) {
    this.config = config;
    this.helixManager = helixManager;
    this.fs = fs;
    this.tokenFilePath = tokenFilePath;
    this.fs.makeQualified(tokenFilePath);
    this.loginIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.LOGIN_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_LOGIN_INTERVAL_IN_MINUTES);
    this.tokenRenewIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.TOKEN_RENEW_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_TOKEN_RENEW_INTERVAL_IN_MINUTES);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("KeytabReLoginExecutor")));
    this.tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("TokenRenewExecutor")));
    this.isHelixClusterManaged = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.IS_HELIX_CLUSTER_MANAGED,
        GobblinClusterConfigurationKeys.DEFAULT_IS_HELIX_CLUSTER_MANAGED);
    this.helixInstanceName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
        GobblinClusterManager.class.getSimpleName());
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the " + this.getClass().getSimpleName());

    LOGGER.info(
        String.format("Scheduling the login task with an interval of %d minute(s)", this.loginIntervalInMinutes));

    // Schedule the Kerberos re-login task
    this.loginExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          loginAndScheduleTokenRenewal();
        }catch(Exception e){
          LOGGER.error("Error during login, will continue the thread and try next time.");
        }
      }
    }, this.loginIntervalInMinutes, this.loginIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Stopping the " + this.getClass().getSimpleName());

    if (this.scheduledTokenRenewTask.isPresent()) {
      this.scheduledTokenRenewTask.get().cancel(true);
    }
    ExecutorsUtils.shutdownExecutorService(this.loginExecutor, Optional.of(LOGGER));
    ExecutorsUtils.shutdownExecutorService(this.tokenRenewExecutor, Optional.of(LOGGER));
  }

  protected void scheduleTokenRenewTask() {
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

  //The whole logic for each re-login
  public void loginAndScheduleTokenRenewal() {
    try {
      // Cancel the currently scheduled token renew task
      if (scheduledTokenRenewTask.isPresent() && scheduledTokenRenewTask.get().cancel(true)) {
        LOGGER.info("Cancelled the token renew task");
      }

      login();
      if (firstLogin) {
        firstLogin = false;
      }

      // Re-schedule the token renew task after re-login
      scheduleTokenRenewTask();
    } catch (IOException | InterruptedException ioe) {
      LOGGER.error("Failed to login from keytab", ioe);
      throw Throwables.propagate(ioe);
    }
  }

  protected void sendTokenFileUpdatedMessage() {
    // Send a message to the controller (when the cluster is not managed)
    // and all the participants if this is not the first login
    if (!this.isHelixClusterManaged) {
      sendTokenFileUpdatedMessage(InstanceType.CONTROLLER);
    }
    sendTokenFileUpdatedMessage(InstanceType.PARTICIPANT);
  }

  /**
   * This method is used to send TokenFileUpdatedMessage which will handle by {@link YarnContainerSecurityManager}
   */
  @VisibleForTesting
  protected void sendTokenFileUpdatedMessage(InstanceType instanceType) {
    sendTokenFileUpdatedMessage(instanceType, "");
  }

  @VisibleForTesting
  protected void sendTokenFileUpdatedMessage(InstanceType instanceType, String instanceName) {
    Criteria criteria = new Criteria();
    criteria.setInstanceName(Strings.isNullOrEmpty(instanceName) ? "%" : instanceName);
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
    GobblinHelixMessagingService messagingService = new GobblinHelixMessagingService(helixManager);

    int messagesSent = messagingService.send(criteria, tokenFileUpdatedMessage);
    LOGGER.info(String.format("Sent %d token file updated message(s) to the %s", messagesSent, instanceType));
  }

  /**
   * Write the current credentials to the token file.
   */
  protected synchronized void writeDelegationTokenToFile(Credentials cred) throws IOException {

    if (this.fs.exists(this.tokenFilePath)) {
      LOGGER.info("Deleting existing token file " + this.tokenFilePath);
      this.fs.delete(this.tokenFilePath, false);
    }

    LOGGER.debug("creating new token file {} with 644 permission.", this.tokenFilePath);

    YarnHelixUtils.writeTokenToFile(this.tokenFilePath, cred, this.fs.getConf());
    // Only grand access to the token file to the login user
    this.fs.setPermission(this.tokenFilePath, new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

    System.setProperty(HADOOP_TOKEN_FILE_LOCATION, tokenFilePath.toUri().getPath());
    LOGGER.info("set HADOOP_TOKEN_FILE_LOCATION = {}", this.tokenFilePath);
  }

  /**
   * Write the current delegation token to the token file. Should be only for testing
   */
  @VisibleForTesting
  protected synchronized void writeDelegationTokenToFile() throws IOException {
    writeDelegationTokenToFile(this.credentials);
  }

  /**
   * Renew the existing delegation token.
   */
  protected abstract void renewDelegationToken() throws IOException, InterruptedException;


  /**
   * Login the user from a given keytab file.
   */
  protected abstract void login() throws IOException, InterruptedException;
}
