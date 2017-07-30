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

import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.util.StringUtils;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;
import gobblin.password.PasswordManager;
import gobblin.util.ConfigUtils;


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
public class AWSClusterSecurityManager {

  private final Config config;

  public AWSClusterSecurityManager(Config config) {
    this.config = config;
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    AWSCredentialsProvider credentialsProviderChain = new DefaultAWSCredentialsProviderChain(this.config);
    if (config.hasPath(GobblinAWSConfigurationKeys.CLIENT_ASSUME_ROLE_KEY) &&
            config.getBoolean(GobblinAWSConfigurationKeys.CLIENT_ASSUME_ROLE_KEY)) {
      String roleArn = config.getString(GobblinAWSConfigurationKeys.CLIENT_ROLE_ARN_KEY);
      String sessionId = config.getString(GobblinAWSConfigurationKeys.CLIENT_SESSION_ID_KEY);
      STSAssumeRoleSessionCredentialsProvider.Builder builder =
              new STSAssumeRoleSessionCredentialsProvider.Builder(roleArn, sessionId)
                      .withLongLivedCredentialsProvider(credentialsProviderChain);
      if (config.hasPath(GobblinAWSConfigurationKeys.CLIENT_EXTERNAL_ID_KEY)) {
        builder.withExternalId(config.getString(GobblinAWSConfigurationKeys.CLIENT_EXTERNAL_ID_KEY));
      }
      if (config.hasPath(GobblinAWSConfigurationKeys.CREDENTIALS_REFRESH_INTERVAL)) {
        builder.withRoleSessionDurationSeconds(
                (int) TimeUnit.MINUTES.toSeconds(config.getLong(GobblinAWSConfigurationKeys.CREDENTIALS_REFRESH_INTERVAL)));
      }
      credentialsProviderChain = builder.build();
    }
    return credentialsProviderChain;
  }

  private static class DefaultAWSCredentialsProviderChain extends AWSCredentialsProviderChain {
    DefaultAWSCredentialsProviderChain(Config config) {
      super(new EnvironmentVariableCredentialsProvider(),
              new SystemPropertiesCredentialsProvider(),
              new ConfigurationCredentialsProvider(config),
              new ProfileCredentialsProvider(),
              new InstanceProfileCredentialsProvider());
    }
  }

  private static class ConfigurationCredentialsProvider implements AWSCredentialsProvider {
    private Config config;

    ConfigurationCredentialsProvider(Config config) {
      this.config = config;
    }

    @Override
    public AWSCredentials getCredentials() {
      String accessKey = null;
      if (config.hasPath(GobblinAWSConfigurationKeys.SERVICE_ACCESS_KEY)) {
        accessKey = config.getString(GobblinAWSConfigurationKeys.SERVICE_ACCESS_KEY);
      }

      String secretKey = null;
      if (config.hasPath(GobblinAWSConfigurationKeys.SERVICE_SECRET_KEY)) {
        secretKey = PasswordManager.getInstance(ConfigUtils.configToState(config))
                .readPassword(config.getString(GobblinAWSConfigurationKeys.SERVICE_SECRET_KEY));
      }

      accessKey = StringUtils.trim(accessKey);
      secretKey = StringUtils.trim(secretKey);
      if (StringUtils.isNullOrEmpty(accessKey) || StringUtils.isNullOrEmpty(secretKey)) {
        throw new AmazonClientException(String.format("Unable to load AWS credentials from config (%s and %s)",
                GobblinAWSConfigurationKeys.SERVICE_ACCESS_KEY, GobblinAWSConfigurationKeys.SERVICE_SECRET_KEY));
      }

      return new BasicAWSCredentials(accessKey, secretKey);
    }

    @Override
    public void refresh() {}

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  }
}
