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
package gobblin.writer.http;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.converter.http.RestEntry;
import gobblin.password.PasswordManager;
import gobblin.writer.DataWriter;
import gobblin.writer.http.SalesforceRestWriter.Operation;

import lombok.AccessLevel;
import lombok.Getter;

/**
 * Builder class that builds SalesForceRestWriter where it takes connection related parameter and type of operation along with the parameters
 * derived from AbstractHttpWriterBuilder
 */
@Getter
public class SalesForceRestWriterBuilder extends AbstractHttpWriterBuilder<Void, RestEntry<JsonObject>, SalesForceRestWriterBuilder>{

  static final String SFDC_PREFIX = "salesforce.";
  static final String CLIENT_ID = SFDC_PREFIX + "client_id";
  static final String CLIENT_SECRET = SFDC_PREFIX + "client_secret";
  static final String USER_ID = SFDC_PREFIX + "user_id";
  static final String PASSWORD = SFDC_PREFIX + "password";
  static final String SFDC_ENCRYPT_KEY_LOC = SFDC_PREFIX + ConfigurationKeys.ENCRYPT_KEY_LOC;
  static final String USE_STRONG_ENCRYPTION = SFDC_PREFIX + "strong_encryption";
  static final String SECURITY_TOKEN = SFDC_PREFIX + "security_token";
  static final String OPERATION = SFDC_PREFIX + "operation";
  static final String BATCH_SIZE = SFDC_PREFIX + "batch_size";
  static final String BATCH_RESOURCE_PATH = SFDC_PREFIX + "batch_resource_path";

  private static final Config FALLBACK = ConfigFactory.parseMap(
        ImmutableMap.<String, String>builder()
          .put(AbstractHttpWriterBuilder.STATIC_SVC_ENDPOINT, "https://login.salesforce.com/services/oauth2/token")
          .put(SECURITY_TOKEN, "")
          .put(BATCH_SIZE, "1")
          .build()
  );

  private String clientId;
  private String clientSecret;
  private String userId;
  private String password;
  private String securityToken;
  private Operation operation;
  private int batchSize;
  private Optional<String> batchResourcePath = Optional.absent();
  @Getter(AccessLevel.NONE) private boolean initializedFromConfig = false;

  @Override
  public SalesForceRestWriterBuilder fromConfig(Config config) {
    super.fromConfig(config);

    initializedFromConfig = true;
    config = config.withFallback(FALLBACK);
    clientId = config.getString(CLIENT_ID);
    clientSecret = config.getString(CLIENT_SECRET);
    userId = config.getString(USER_ID);
    password = config.getString(PASSWORD);
    securityToken = config.getString(SECURITY_TOKEN);
    operation = Operation.valueOf(config.getString(OPERATION).toUpperCase());
    batchSize = config.getInt(BATCH_SIZE);
    Preconditions.checkArgument(batchSize > 0, BATCH_SIZE + " cannot be negative: " + batchSize);

    if (batchSize > 1) {
      batchResourcePath = Optional.of(config.getString(BATCH_RESOURCE_PATH));
    }

    if (config.hasPath(SFDC_ENCRYPT_KEY_LOC)) {
      Properties props = new Properties();
      if (config.hasPath(USE_STRONG_ENCRYPTION)) {
        props.put(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, config.getString(USE_STRONG_ENCRYPTION));
      }

      props.put(ConfigurationKeys.ENCRYPT_KEY_LOC, config.getString(SFDC_ENCRYPT_KEY_LOC));
      password = PasswordManager.getInstance(props).readPassword(password);
    }
    return typedSelf();
  }

  @Override
  public DataWriter<RestEntry<JsonObject>> build() throws IOException {
    validate();
    //From config is the only path to set the config and also validates required properties.
    Preconditions.checkArgument(initializedFromConfig, this.getClass().getSimpleName() + " must be build via fromConfig method.");
    Preconditions.checkArgument(getSvcEndpoint().isPresent(), "Service end point is required for Oauth2 end point of Salesforce.com");

    return new SalesforceRestWriter(this);
  }
}
