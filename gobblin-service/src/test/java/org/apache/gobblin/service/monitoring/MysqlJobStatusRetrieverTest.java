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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MysqlJobStatusRetrieverTest extends JobStatusRetrieverTest {
  private MysqlJobStatusStateStore dbJobStateStore;
  private static final String TEST_USER = "testUser";
  private static final String TEST_PASSWORD = "testPassword";

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    ITestMetastoreDatabase testMetastoreDatabase = TestMetastoreDatabaseFactory.get();
    String jdbcUrl = testMetastoreDatabase.getJdbcUrl();

    ConfigBuilder configBuilder = ConfigBuilder.create();
    configBuilder.addPrimitive(MysqlJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, jdbcUrl);
    configBuilder.addPrimitive(MysqlJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, TEST_USER);
    configBuilder.addPrimitive(MysqlJobStatusRetriever.CONF_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, TEST_PASSWORD);

    this.jobStatusRetriever = new MysqlJobStatusRetriever(configBuilder.build());
    this.dbJobStateStore = ((MysqlJobStatusRetriever) this.jobStatusRetriever).getStateStore();
    cleanUpDir();
  }

  @Test
  public void testGetJobStatusesForFlowExecution() throws IOException {
    super.testGetJobStatusesForFlowExecution();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution")
  public void testJobTiming() throws Exception {
    super.testJobTiming();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testOutOfOrderJobTimingEvents() throws IOException {
    super.testOutOfOrderJobTimingEvents();
  }

  @Test (dependsOnMethods = "testJobTiming")
  public void testGetJobStatusesForFlowExecution1() {
    super.testGetJobStatusesForFlowExecution1();
  }

  @Test (dependsOnMethods = "testGetJobStatusesForFlowExecution1")
  public void testGetLatestExecutionIdsForFlow() throws Exception {
    super.testGetLatestExecutionIdsForFlow();
  }

  @Override
  void cleanUpDir() throws Exception {
    this.dbJobStateStore.delete(KafkaJobStatusMonitor.jobStatusStoreName(FLOW_GROUP, FLOW_NAME));
  }
}
