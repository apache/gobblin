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

package org.apache.gobblin.util.jdbc;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

import java.util.Properties;

import javax.sql.DataSource;

import lombok.ToString;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


@ToString(exclude = "passWord")
public class DataSourceBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DataSourceBuilder.class);

  private String url;
  private String driver;
  private String userName;
  private String passWord;
  private Integer maxIdleConnections;
  private Integer maxActiveConnections;
  private String cryptoKeyLocation;
  private Boolean useStrongEncryption;
  private State state;

  public static DataSourceBuilder builder() {
    return new DataSourceBuilder();
  }

  public DataSourceBuilder url(String url) {
    this.url = url;
    return this;
  }

  public DataSourceBuilder driver(String driver) {
    this.driver = driver;
    return this;
  }

  public DataSourceBuilder userName(String userName) {
    this.userName = userName;
    return this;
  }

  public DataSourceBuilder passWord(String passWord) {
    this.passWord = passWord;
    return this;
  }

  public DataSourceBuilder maxIdleConnections(int maxIdleConnections) {
    this.maxIdleConnections = maxIdleConnections;
    return this;
  }

  public DataSourceBuilder maxActiveConnections(int maxActiveConnections) {
    this.maxActiveConnections = maxActiveConnections;
    return this;
  }

  public DataSourceBuilder cryptoKeyLocation(String cryptoKeyLocation) {
    this.cryptoKeyLocation = cryptoKeyLocation;
    return this;
  }

  public DataSourceBuilder useStrongEncryption(boolean useStrongEncryption) {
    this.useStrongEncryption = useStrongEncryption;
    return this;
  }

  public DataSourceBuilder state(State state) {
    this.state = state;
    return this;
  }

  public DataSource build() {
    validate();
    Properties properties = new Properties();
    if (this.state != null) {
      properties = this.state.getProperties();
    }
    properties.setProperty(DataSourceProvider.CONN_URL, this.url);
    properties.setProperty(DataSourceProvider.USERNAME, this.userName);
    properties.setProperty(DataSourceProvider.PASSWORD, this.passWord);
    properties.setProperty(DataSourceProvider.CONN_DRIVER, this.driver);
    if (!StringUtils.isEmpty(this.cryptoKeyLocation)) {
      properties.setProperty(ConfigurationKeys.ENCRYPT_KEY_LOC, this.cryptoKeyLocation);
    }

    if (this.maxIdleConnections != null) {
      properties.setProperty(DataSourceProvider.MAX_IDLE_CONNS, this.maxIdleConnections.toString());
    }

    if (this.maxActiveConnections != null) {
      properties.setProperty(DataSourceProvider.MAX_ACTIVE_CONNS, this.maxActiveConnections.toString());
    }

    if (this.useStrongEncryption != null) {
      properties.setProperty(ConfigurationKeys.ENCRYPT_USE_STRONG_ENCRYPTOR, this.useStrongEncryption.toString());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Building DataSource with properties " + properties);
    }

    return new DataSourceProvider(properties).get();
  }

  private void validate() {
    validateNotEmpty(this.url, "url");
    validateNotEmpty(this.driver, "driver");
    validateNotEmpty(this.passWord, "passWord");
    Preconditions.checkArgument(this.maxIdleConnections == null || this.maxIdleConnections > 0,
        "maxIdleConnections should be a positive integer.");
    Preconditions.checkArgument(this.maxActiveConnections == null || this.maxActiveConnections > 0,
        "maxActiveConnections should be a positive integer.");
  }

  private static void validateNotEmpty(String s, String name) {
    Preconditions.checkArgument(!StringUtils.isEmpty(s), name + " should not be empty.");
  }
}
