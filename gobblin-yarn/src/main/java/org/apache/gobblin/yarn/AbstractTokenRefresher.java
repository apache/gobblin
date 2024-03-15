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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;


/**
 * <p>
 *   The super class for key management
 *   This class uses a scheduled task to do re-login to re-fetch token on a
 *   configurable schedule. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewal interval are configurable.
 * </p>
 */
@Slf4j
public abstract class AbstractTokenRefresher extends AbstractIdleService {

  protected Config config;

  protected final FileSystem fs;
  protected final Path tokenFilePath;

  protected Credentials credentials = new Credentials();
  private final long loginIntervalInMinutes;
  private final long tokenRenewIntervalInMinutes;
  private final ScheduledExecutorService loginExecutor;
  private final ScheduledExecutorService tokenRenewExecutor;

  private Optional<ScheduledFuture<?>> scheduledTokenRenewTask = Optional.absent();

  // This flag is used to tell if this is the first login. If yes, no token updated message will be
  // sent to the controller and the participants as they may not be up running yet. The first login
  // happens after this class starts up so the token gets regularly refreshed before the next login.
  protected volatile boolean firstLogin = true;

  public AbstractTokenRefresher(Config config, FileSystem fs, Path tokenFilePath) {
    this.config = config;
    this.fs = fs;
    this.tokenFilePath = tokenFilePath;
    this.fs.makeQualified(tokenFilePath);
    this.loginIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.LOGIN_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_LOGIN_INTERVAL_IN_MINUTES);
    this.tokenRenewIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.TOKEN_RENEW_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_TOKEN_RENEW_INTERVAL_IN_MINUTES);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("KeytabReLoginExecutor")));
    this.tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("TokenRenewExecutor")));
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Starting the {}", this.getClass().getSimpleName());
    log.info("Scheduling the login task with an interval of {} minute(s)", this.loginIntervalInMinutes);

    // Schedule the Kerberos re-login task
    this.loginExecutor.scheduleAtFixedRate(() -> {
      try {
        loginAndScheduleTokenRenewal();
      } catch (Exception e){
        log.error("Error during login, will continue the thread and try next time.");
      }
    }, this.loginIntervalInMinutes, this.loginIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected synchronized void shutDown() throws Exception {
    log.info("Stopping the {}", this.getClass().getSimpleName());

    if (this.scheduledTokenRenewTask.isPresent()) {
      this.scheduledTokenRenewTask.get().cancel(true);
    }
    ExecutorsUtils.shutdownExecutorService(this.loginExecutor, Optional.of(log));
    ExecutorsUtils.shutdownExecutorService(this.tokenRenewExecutor, Optional.of(log));
  }

  protected void scheduleTokenRenewTask() {
    log.info("Scheduling the token renew task with an interval of {} minute(s)", this.tokenRenewIntervalInMinutes);

    this.scheduledTokenRenewTask = Optional.of(
        this.tokenRenewExecutor.scheduleAtFixedRate(() -> {
          try {
            renewDelegationToken();
          } catch (IOException ioe) {
            log.error("Failed to renew delegation token", ioe);
            throw new RuntimeException(ioe);
          } catch (InterruptedException ie) {
            log.error("Token renew task has been interrupted");
            Thread.currentThread().interrupt();
          }
        }, this.tokenRenewIntervalInMinutes, this.tokenRenewIntervalInMinutes, TimeUnit.MINUTES));
  }

  //The whole logic for each re-login
  public synchronized void loginAndScheduleTokenRenewal() {
    try {
      // Cancel the currently scheduled token renew task
      if (scheduledTokenRenewTask.isPresent() && scheduledTokenRenewTask.get().cancel(true)) {
        log.info("Cancelled the token renew task");
      }

      login();
      if (firstLogin) {
        firstLogin = false;
      }

      // Re-schedule the token renew task after re-login
      scheduleTokenRenewTask();
    } catch (IOException | InterruptedException ioe) {
      log.error("Failed to login from keytab", ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Write the current credentials to the token file.
   */
  protected synchronized void writeDelegationTokenToFile(Credentials cred) throws IOException {

    if (this.fs.exists(this.tokenFilePath)) {
      log.info("Deleting existing token file " + this.tokenFilePath);
      this.fs.delete(this.tokenFilePath, false);
    }

    FsPermission permission = new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    log.debug("Creating new token file {} with permission={}.", this.tokenFilePath, permission);

    YarnHelixUtils.writeTokenToFile(this.tokenFilePath, cred, this.fs.getConf());
    // Only grant access to the token file to the login user
    this.fs.setPermission(this.tokenFilePath, permission);

    System.setProperty(HADOOP_TOKEN_FILE_LOCATION, tokenFilePath.toUri().getPath());
    log.info("set HADOOP_TOKEN_FILE_LOCATION = {}", this.tokenFilePath);
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
