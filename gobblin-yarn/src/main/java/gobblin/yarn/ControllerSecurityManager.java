/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

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
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;

import com.typesafe.config.Config;

import gobblin.util.ExecutorsUtils;


/**
 * A class for managing Kerberos login and token renewing in the Controller.
 *
 * <p>
 *   This class uses a scheduled task to do Kerberos re-login to renew the Kerberos ticket on a
 *   configurable schedule if login is from a keytab file. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewing interval are configurable.
 * </p>
 *
 * @author ynli
 */
public class ControllerSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerSecurityManager.class);

  private final Config config;

  private final HelixManager helixManager;
  private final FileSystem fs;
  private final Path tokenFilePath;
  private UserGroupInformation currentUser;
  private Token<? extends TokenIdentifier> token;

  private final long loginIntervalInHours;
  private final long tokenRenewIntervalInHours;

  private final ScheduledExecutorService loginExecutor;
  private final ScheduledExecutorService tokenRenewExecutor;
  private ScheduledFuture<?> scheduledTokenRenewTask;

  public ControllerSecurityManager(Config config, HelixManager helixManager, Path appWorkDir, FileSystem fs)
      throws IOException {
    this.config = config;
    this.helixManager = helixManager;
    this.fs = fs;

    this.tokenFilePath = config.hasPath(ConfigurationConstants.TOKEN_FILE_PATH) ?
        new Path(config.getString(ConfigurationConstants.TOKEN_FILE_PATH)) :
        new Path(appWorkDir, ConfigurationConstants.TOKEN_FILE_EXTENSION);
    this.fs.makeQualified(tokenFilePath);
    this.currentUser = UserGroupInformation.getCurrentUser();
    this.loginIntervalInHours = config.getLong(ConfigurationConstants.LOGIN_INTERVAL_IN_HOURS);
    this.tokenRenewIntervalInHours = config.getLong(ConfigurationConstants.TOKEN_RENEW_INTERVAL_IN_HOURS);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("KeytabReLoginExecutor")));
    this.tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("TokenRenewExecutor")));

    // Initial login
    login();

    // Schedule the token renew task
    scheduleTokenRenewTask();
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Scheduling the Kerberos keytab login task");

    // Schedule the Kerberos re-login task
    this.loginExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          // Cancel the currently scheduled token renew task
          if (scheduledTokenRenewTask.cancel(true)) {
            LOGGER.info("Cancelled the token renew task");
          }
          reLogin();
          // Re-schedule the token renew task after re-login
          scheduleTokenRenewTask();
        } catch (IOException ioe) {
          throw Throwables.propagate(ioe);
        }
      }
    }, this.loginIntervalInHours, this.loginIntervalInHours, TimeUnit.HOURS);
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorsUtils.shutdownExecutorService(this.loginExecutor, Optional.of(LOGGER));
    ExecutorsUtils.shutdownExecutorService(this.tokenRenewExecutor, Optional.of(LOGGER));
  }

  /**
   * Login the user initially.
   */
  private void login() throws IOException {
    if (this.config.hasPath(ConfigurationConstants.KEYTAB_FILE_PATH)) {
      // It is assumed that the presence of the configuration property for the
      // keytab file path indicates the login should happen through a keytab.
      loginFromKeytab();
    }
  }

  /**
   * Re-login the current logged-in user.
   */
  private void reLogin() throws IOException {
    if (this.currentUser.isFromKeytab()) {
      // Re-login from the keytab if the initial login is from a keytab
      reLoginFromKeytab();
    }
  }

  private void scheduleTokenRenewTask() {
    LOGGER.info("Scheduling the token renew task");

    this.scheduledTokenRenewTask = this.tokenRenewExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          renewDelegationToken();
        } catch (IOException ioe) {
          throw Throwables.propagate(ioe);
        } catch (InterruptedException ie) {
          LOGGER.error("Token renew task has been interrupted");
          Thread.currentThread().interrupt();
        }
      }
    }, this.tokenRenewIntervalInHours, this.tokenRenewIntervalInHours, TimeUnit.HOURS);
  }

  /**
   * Renew the existing delegation token.
   */
  private synchronized void renewDelegationToken() throws IOException, InterruptedException {
    this.token.renew(this.fs.getConf());
    writeDelegationTokenToFile();
  }

  /**
   * Get a new delegation token for the current logged-in user.
   */
  private synchronized void getNewDelegationTokenForLoginUser() throws IOException {
    this.token = this.fs.getDelegationToken(this.currentUser.getShortUserName());
  }

  /**
   * Login the user from a given keytab file.
   */
  private void loginFromKeytab() throws IOException {
    String keyTabFilePath = this.config.getString(ConfigurationConstants.KEYTAB_FILE_PATH);
    if (Strings.isNullOrEmpty(keyTabFilePath)) {
      throw new IOException("Keytab file path is not defined for Kerberos login");
    }

    String principal = this.config.getString(ConfigurationConstants.KEYTAB_PRINCIPAL_NAME);
    if (Strings.isNullOrEmpty(principal)) {
      principal = this.currentUser.getShortUserName() + "/localhost@LOCALHOST";
    }

    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication",
        UserGroupInformation.AuthenticationMethod.KERBEROS.toString().toLowerCase());
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);
    LOGGER.info(String.format("Logged in from keytab file %s using principal %s", keyTabFilePath, principal));

    this.currentUser = UserGroupInformation.getCurrentUser();

    getNewDelegationTokenForLoginUser();
    writeDelegationTokenToFile();
  }

  /**
   * Re-login the current login user from the keytab file used for the initial login.
   */
  private void reLoginFromKeytab() throws IOException {
    this.currentUser.reloginFromKeytab();
    getNewDelegationTokenForLoginUser();
    writeDelegationTokenToFile();
  }

  /**
   * Write the current delegation token to the token file.
   */
  private synchronized void writeDelegationTokenToFile() throws IOException {
    if (this.fs.exists(this.tokenFilePath)) {
      LOGGER.info("Deleting existing token file " + this.tokenFilePath);
      this.fs.delete(this.tokenFilePath, false);
    }

    LOGGER.info("Writing new or renewed token to token file " + this.tokenFilePath);
    YarnHelixUtils.writeTokenToFile(this.token, this.tokenFilePath, this.fs.getConf());
    // Only grand access to the token file to the login user
    this.fs.setPermission(this.tokenFilePath, new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

    // Send a message to all the participants
    sendTokenFileUpdatedMessage();
  }

  private void sendTokenFileUpdatedMessage() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
    criteria.setSessionSpecific(true);

    Message tokenFileUpdatedMessage = new Message(Message.MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
    tokenFileUpdatedMessage.setMsgSubType(HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());
    tokenFileUpdatedMessage.setMsgState(Message.MessageState.NEW);
    tokenFileUpdatedMessage.setResourceId(ResourceId.from(this.tokenFilePath.toString()));

    int messagesSent = this.helixManager.getMessagingService().send(criteria, tokenFileUpdatedMessage);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the participants",
          tokenFileUpdatedMessage.getMsgSubType()));
    }
  }
}
