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

  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.quotaManager = new MysqlUserQuotaManager(config);
  }

  @Test
  public void testIncreaseCount() throws Exception {
    int prevCount = this.quotaManager.incrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    Assert.assertEquals(prevCount, 0);

    prevCount = this.quotaManager.incrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    Assert.assertEquals(prevCount, 1);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 2);

    prevCount = this.quotaManager.incrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    Assert.assertEquals(prevCount, 0);

    prevCount = this.quotaManager.incrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    Assert.assertEquals(prevCount, 1);
  }

  @Test(dependsOnMethods = "testIncreaseCount")
  public void testDecreaseCount() throws Exception {
    this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 1);

    this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 0);

    this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 0);

    this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT), 1);
    this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.FLOWGROUP_COUNT);
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
      while (i++ < 1000) {
        try {
          if (increaseOrDecrease) {
            MysqlUserQuotaManagerTest.this.quotaManager.incrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
          } else {
            MysqlUserQuotaManagerTest.this.quotaManager.decrementJobCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT);
          }
        } catch (IOException e) {
          Assert.fail("Thread got an exception.", e);
        }
      }
    }
  }

  @Test(dependsOnMethods = "testDecreaseCount")
  public void testConcurrentChanges() throws IOException, InterruptedException {
    Runnable increaseCountRunnable = new ChangeCountRunnable(true);
    Runnable decreaseCountRunnable = new ChangeCountRunnable(false);
    Thread thread1 = new Thread(increaseCountRunnable);
    Thread thread2 = new Thread(increaseCountRunnable);
    Thread thread3 = new Thread(increaseCountRunnable);
    Thread thread4 = new Thread(decreaseCountRunnable);
    Thread thread5 = new Thread(decreaseCountRunnable);
    Thread thread6 = new Thread(decreaseCountRunnable);

    thread1.start();
    thread2.start();
    thread3.start();
    thread1.join();
    thread2.join();
    thread3.join();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), 3000);
    thread4.start();
    thread5.start();
    thread6.start();
    thread4.join();
    thread5.join();
    thread6.join();
    Assert.assertEquals(this.quotaManager.getCount(PROXY_USER, AbstractUserQuotaManager.CountType.USER_COUNT), -1);
  }
}
