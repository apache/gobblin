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

package gobblin.aws;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.util.ExecutorsUtils;

/**
 * Class for managing AWS login and credentials renewal.
 *
 * <p>
 *   This class makes use of {@link BasicAWSCredentials} and {@link BasicSessionCredentials} to
 *   manage renewal of Amazon AWS authentication.
 *   This class runs a scheduled login executor
 *   task to refresh the credentials upon specified renewal interval which is configurable.
 * </p>
 *
 * @author Abhishek Tiwari
 */
@Alpha
public class AWSClusterSecurityManager extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AWSClusterSecurityManager.class);

  private final Config config;

  private volatile String serviceAccessKey;
  private volatile String serviceSecretKey;
  private volatile boolean clientAssumeRole;
  private volatile String clientRoleArn;
  private volatile String clientExternalId;
  private volatile String clientSessionId;
  private volatile long lastRefreshTimeInMillis;

  private volatile BasicAWSCredentials basicAWSCredentials;
  private volatile BasicSessionCredentials basicSessionCredentials;

  private final long refreshIntervalInMinutes;

  private final ScheduledExecutorService loginExecutor;

  public AWSClusterSecurityManager(Config config) {
    this.config = config;

    this.refreshIntervalInMinutes = config.getLong(GobblinAWSConfigurationKeys.CREDENTIALS_REFRESH_INTERVAL);

    this.loginExecutor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("LoginExecutor")));
  }

  private void fetchLoginConfiguration() {
    this.serviceAccessKey = config.getString(GobblinAWSConfigurationKeys.SERVICE_ACCESS_KEY);
    this.serviceSecretKey = config.getString(GobblinAWSConfigurationKeys.SERVICE_SECRET_KEY);
    this.clientAssumeRole = config.getBoolean(GobblinAWSConfigurationKeys.CLIENT_ASSUME_ROLE_KEY);

    // If we are running on behalf of another AWS user, we need to fetch temporary credentials for a
    // .. configured role
    if (this.clientAssumeRole) {
      this.clientRoleArn = config.getString(GobblinAWSConfigurationKeys.CLIENT_ROLE_ARN_KEY);
      this.clientExternalId = config.getString(GobblinAWSConfigurationKeys.CLIENT_EXTERNAL_ID_KEY);
      this.clientSessionId = config.getString(GobblinAWSConfigurationKeys.CLIENT_SESSION_ID_KEY);
    }
  }

  @Override
  protected void startUp()
      throws Exception {
    LOGGER.info("Starting the " + AWSClusterSecurityManager.class.getSimpleName());

    LOGGER.info(
        String.format("Scheduling the credentials refresh task with an interval of %d minute(s)",
            this.refreshIntervalInMinutes));

    // Schedule the login task
    this.loginExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          login();
        } catch (IOException ioe) {
          LOGGER.error("Failed to login", ioe);
          throw Throwables.propagate(ioe);
        }
      }
    }, 0, this.refreshIntervalInMinutes, TimeUnit.MINUTES);
  }

  @Override
  protected void shutDown()
      throws Exception {
    GobblinAWSUtils.shutdownExecutorService(this.getClass(), this.loginExecutor, LOGGER);
  }

  private void login() throws IOException {
    // Refresh login configuration details from config
    fetchLoginConfiguration();

    // Primary AWS user login
    this.basicAWSCredentials = new BasicAWSCredentials(this.serviceAccessKey, this.serviceSecretKey);

    // If running on behalf of another AWS user,
    // .. assume role as configured
    if (this.clientAssumeRole) {
      AssumeRoleRequest assumeRoleRequest = new AssumeRoleRequest()
          .withRoleSessionName(this.clientSessionId)
          .withExternalId(this.clientExternalId)
          .withRoleArn(this.clientRoleArn);

      final AWSSecurityTokenServiceClient stsClient = new AWSSecurityTokenServiceClient(this.basicAWSCredentials);

      final AssumeRoleResult assumeRoleResult = stsClient.assumeRole(assumeRoleRequest);

      this.basicSessionCredentials = new BasicSessionCredentials(
          assumeRoleResult.getCredentials().getAccessKeyId(),
          assumeRoleResult.getCredentials().getSecretAccessKey(),
          assumeRoleResult.getCredentials().getSessionToken()
      );
    }

    this.lastRefreshTimeInMillis = System.currentTimeMillis();
  }

  public long getLastRefreshTimeInMillis() {
    return lastRefreshTimeInMillis;
  }

  public boolean isAssumeRoleEnabled() {
    return this.clientAssumeRole;
  }

  public BasicAWSCredentials getBasicAWSCredentials() {
    return this.basicAWSCredentials;
  }

  public BasicSessionCredentials getBasicSessionCredentials() {
    if (!this.clientAssumeRole) {
      throw new IllegalStateException("AWS Security manager is not configured to run on behalf of another AWS user. "
          + "Use getBasicAWSCredentials() instead");
    }
    return this.basicSessionCredentials;
  }
}
