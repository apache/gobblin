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

import java.io.IOException;
import javax.sql.DataSource;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;

/**
 * Unit tests for {@link MysqlDataSourceFactory}.
 */
@Test(groups = { "gobblin.metastore" })
public class MysqlDataSourceFactoryTest {

  @Test
  public void testSameKey() throws IOException {

    Config config = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.STATE_STORE_DB_URL_KEY, "url",
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, "user",
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "dummypwd"));

    DataSource dataSource1 = MysqlDataSourceFactory.get(config,
        SharedResourcesBrokerFactory.getImplicitBroker());

    DataSource dataSource2 = MysqlDataSourceFactory.get(config,
        SharedResourcesBrokerFactory.getImplicitBroker());

    Assert.assertEquals(dataSource1, dataSource2);
  }

  @Test
  public void testDifferentKey() throws IOException {

    Config config1 = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.STATE_STORE_DB_URL_KEY, "url1",
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, "user",
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "dummypwd"));

    Config config2 = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.STATE_STORE_DB_URL_KEY, "url2",
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, "user",
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "dummypwd"));

    DataSource dataSource1 = MysqlDataSourceFactory.get(config1,
        SharedResourcesBrokerFactory.getImplicitBroker());

    DataSource dataSource2 = MysqlDataSourceFactory.get(config2,
        SharedResourcesBrokerFactory.getImplicitBroker());

    Assert.assertNotEquals(dataSource1, dataSource2);
  }

  @Test
  public void testSameDbDifferentUser() throws IOException {

    Config config1 = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.STATE_STORE_DB_URL_KEY, "url1",
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, "user1",
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "dummypwd"));

    Config config2 = ConfigFactory.parseMap(ImmutableMap.of(ConfigurationKeys.STATE_STORE_DB_URL_KEY, "url1",
        ConfigurationKeys.STATE_STORE_DB_USER_KEY, "user2",
        ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, "dummypwd"));

    DataSource dataSource1 = MysqlDataSourceFactory.get(config1,
        SharedResourcesBrokerFactory.getImplicitBroker());

    DataSource dataSource2 = MysqlDataSourceFactory.get(config2,
        SharedResourcesBrokerFactory.getImplicitBroker());

    Assert.assertNotEquals(dataSource1, dataSource2);
  }
}
