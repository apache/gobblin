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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;

import java.sql.Connection;
import java.sql.SQLException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;

public class MysqlUserQuotaManagerTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "quotas";
  private static final String PROXY_USER = "abora";
  private MysqlUserQuotaManager quotaManager;
  public static int INCREMENTS = 1000;

  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(MysqlUserQuotaManager.CONFIG_PREFIX + '.' + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(MysqlUserQuotaManager.CONFIG_PREFIX + '.' + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(MysqlUserQuotaManager.CONFIG_PREFIX + '.' + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(MysqlUserQuotaManager.CONFIG_PREFIX + '.' + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.quotaManager = new MysqlUserQuotaManager(config);
  }

  @Test
  public void testRunningDagStore() throws Exception {
    String dagId = DagManagerUtils.generateDagId(DagManagerTest.buildDag("dagId", 1234L, "", 1).getNodes().get(0)).toString();
    Connection connection = this.quotaManager.quotaStore.dataSource.getConnection();
    Assert.assertFalse(this.quotaManager.containsDagId(dagId));
    this.quotaManager.addDagId(connection, dagId);
    connection.commit();
    Assert.assertTrue(this.quotaManager.containsDagId(dagId));
    Assert.assertTrue(this.quotaManager.removeDagId(connection, dagId));
    connection.commit();
    Assert.assertFalse(this.quotaManager.containsDagId(dagId));
    Assert.assertFalse(this.quotaManager.removeDagId(connection, dagId));
    connection.commit();
    connection.close();
  }

    @Test
  public void testIncreaseCount() throws Exception {
    Connection connection = this.quotaManager.quotaStore.dataSource.getConnection();
    int prevCount = this.quotaManager.incrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    connection.commit();
    Assert.assertEquals(prevCount, 0);

    prevCount = this.quotaManager.incrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    connection.commit();
    Assert.assertEquals(prevCount, 1);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 2);

    prevCount = this.quotaManager.incrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    connection.commit();
    Assert.assertEquals(prevCount, 0);

    prevCount = this.quotaManager.incrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    connection.commit();
    Assert.assertEquals(prevCount, 1);
    connection.close();
  }

  @Test(dependsOnMethods = "testIncreaseCount")
  public void testDecreaseCount() throws Exception {
    Connection connection = this.quotaManager.quotaStore.dataSource.getConnection();
    this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    connection.commit();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 1);

    this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    connection.commit();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 0);

    this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    connection.commit();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 0);

    this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    connection.commit();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT), 1);
    this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    connection.commit();
    // on count reduced to zero, the row should get deleted and the get call should return -1 instead of 0.
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT), -1);
  }

  class ChangeCountRunnable implements Runnable {
    boolean increaseOrDecrease;

    public ChangeCountRunnable(boolean increaseOrDecrease) {
      this.increaseOrDecrease = increaseOrDecrease;
    }

    @Override
    public void run() {
      int i = 0;
      while (i++ < INCREMENTS) {
        try (Connection connection = MysqlUserQuotaManagerTest.this.quotaManager.quotaStore.dataSource.getConnection();) {
          if (increaseOrDecrease) {
            MysqlUserQuotaManagerTest.this.quotaManager.incrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
          } else {
            MysqlUserQuotaManagerTest.this.quotaManager.decrementJobCount(connection, PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
          }
          connection.commit();
        } catch (IOException | SQLException e) {
          Assert.fail("Thread got an exception.", e);
        }
      }
    }
  }

  @Test(dependsOnMethods = "testDecreaseCount")
  public void testConcurrentChanges() throws IOException, InterruptedException {
    int numOfThreads = 3;
    Thread thread1 = new Thread(new ChangeCountRunnable(true));
    Thread thread2 = new Thread(new ChangeCountRunnable(true));
    Thread thread3 = new Thread(new ChangeCountRunnable(true));
    Thread thread4 = new Thread(new ChangeCountRunnable(false));
    Thread thread5 = new Thread(new ChangeCountRunnable(false));
    Thread thread6 = new Thread(new ChangeCountRunnable(false));

    thread1.start();
    thread2.start();
    thread3.start();
    thread1.join();
    thread2.join();
    thread3.join();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT),
        INCREMENTS * 3);
    thread4.start();
    thread5.start();
    thread6.start();
    thread4.join();
    thread5.join();
    thread6.join();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), -1);
  }
}
