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
package org.apache.gobblin.metastore;

import javax.sql.DataSource;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.configuration.ConfigurationKeys;

/**
 * Unit tests for {@link MysqlStateStore}.
 */
@Test(groups = { "gobblin.metastore" })
public class MysqlStateStoreTest {

  private static final String TEST_JDBC_URL = "jdbc:mysql://localhost:3306/test";
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @Test
  public void testNewDataSourceWithoutSslMode() {
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        ConfigurationKeys.STATE_STORE_DB_URL_KEY, TEST_JDBC_URL,
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER,
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD
    ));

    DataSource dataSource = MysqlStateStore.newDataSource(config);

    Assert.assertNotNull(dataSource, "DataSource should not be null");
    Assert.assertTrue(dataSource instanceof HikariDataSource, "DataSource should be HikariDataSource");

    HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
    Assert.assertEquals(hikariDataSource.getJdbcUrl(), TEST_JDBC_URL, "JDBC URL should match");
    Assert.assertEquals(hikariDataSource.getUsername(), TEST_USER, "Username should match");

    // SSL mode should not be set when the configuration is not provided (defaults to false)
    Object sslModeProperty = hikariDataSource.getDataSourceProperties().get("sslMode");
    Assert.assertNull(sslModeProperty, "sslMode property should not be set when config is not provided");

    hikariDataSource.close();
  }

  @Test
  public void testNewDataSourceWithSslModeEnabled() {
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        ConfigurationKeys.STATE_STORE_DB_URL_KEY, TEST_JDBC_URL,
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER,
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD,
        ConfigurationKeys.STATE_STORE_DB_SSL_MODE_ENABLED, "true"
    ));

    DataSource dataSource = MysqlStateStore.newDataSource(config);

    Assert.assertNotNull(dataSource, "DataSource should not be null");
    Assert.assertTrue(dataSource instanceof HikariDataSource, "DataSource should be HikariDataSource");

    HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
    Assert.assertEquals(hikariDataSource.getJdbcUrl(), TEST_JDBC_URL, "JDBC URL should match");
    Assert.assertEquals(hikariDataSource.getUsername(), TEST_USER, "Username should match");

    // Verify that SSL mode property is set to VERIFY_IDENTITY
    Assert.assertNotNull(hikariDataSource.getDataSourceProperties(), "DataSource properties should not be null");
    Object sslModeProperty = hikariDataSource.getDataSourceProperties().get("sslMode");
    Assert.assertNotNull(sslModeProperty, "sslMode property should be set when SSL mode is enabled");
    Assert.assertEquals(sslModeProperty, "VERIFY_IDENTITY", "sslMode should be set to VERIFY_IDENTITY");

    hikariDataSource.close();
  }

  @Test
  public void testNewDataSourceWithSslModeDisabled() {
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        ConfigurationKeys.STATE_STORE_DB_URL_KEY, TEST_JDBC_URL,
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER,
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD,
        ConfigurationKeys.STATE_STORE_DB_SSL_MODE_ENABLED, "false"
    ));

    DataSource dataSource = MysqlStateStore.newDataSource(config);

    Assert.assertNotNull(dataSource, "DataSource should not be null");
    Assert.assertTrue(dataSource instanceof HikariDataSource, "DataSource should be HikariDataSource");

    HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
    Assert.assertEquals(hikariDataSource.getJdbcUrl(), TEST_JDBC_URL, "JDBC URL should match");
    Assert.assertEquals(hikariDataSource.getUsername(), TEST_USER, "Username should match");

    // Verify that SSL mode property is not set when explicitly disabled
    Object sslModeProperty = hikariDataSource.getDataSourceProperties().get("sslMode");
    Assert.assertNull(sslModeProperty, "sslMode property should not be set when SSL mode is disabled");

    hikariDataSource.close();
  }
}

