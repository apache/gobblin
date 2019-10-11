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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *   The super class for key management
 *   This class uses a scheduled task to do re-login to refetch token on a
 *   configurable schedule. It also uses a second scheduled task
 *   to renew the delegation token after each login. Both the re-login interval and the token
 *   renewing interval are configurable.
 */
public abstract class YarnAppSecurityManager extends AbstractIdleService {

  protected Logger LOGGER = LoggerFactory.getLogger(this.getClass().getName());

  protected Config config;

  private final long loginIntervalInMinutes;
  private final long tokenRenewIntervalInMinutes;

  private final ScheduledExecutorService loginExecutor;
  private final ScheduledExecutorService tokenRenewExecutor;
  private Optional<ScheduledFuture<?>> scheduledTokenRenewTask = Optional.absent();

  // This flag is used to tell if this is the first login. If yes, no token updated message will be
  // sent to the controller and the participants as they may not be up running yet. The first login
  // happens after this class starts up so the token gets regularly refreshed before the next login.
  public volatile boolean firstLogin = true;

  public YarnAppSecurityManager(Config config) {
    this.config = config;
    this.loginIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.LOGIN_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_LOGIN_INTERVAL_IN_MINUTES);
    this.tokenRenewIntervalInMinutes = ConfigUtils.getLong(config, GobblinYarnConfigurationKeys.TOKEN_RENEW_INTERVAL_IN_MINUTES,
        GobblinYarnConfigurationKeys.DEFAULT_TOKEN_RENEW_INTERVAL_IN_MINUTES);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("KeytabReLoginExecutor")));
    this.tokenRenewExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("TokenRenewExecutor")));
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
        loginAdnScheduleTokenRenew();
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
  public void loginAdnScheduleTokenRenew() {
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

  /**
   * Renew the existing delegation token.
   */
  protected abstract void renewDelegationToken() throws IOException, InterruptedException;


  /**
   * Login the user from a given keytab file.
   */
  protected abstract void login() throws IOException, InterruptedException;
}
