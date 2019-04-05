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

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.service.ExecutionStatus;


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

  @Override
  public void testJobTiming() throws Exception {
    addJobStatusToStateStore(1234L, MY_JOB_NAME_1, ExecutionStatus.COMPLETE.name(), JOB_END_TIME, JOB_END_TIME);
    Iterator<JobStatus>
        jobStatusIterator = this.jobStatusRetriever.getJobStatusesForFlowExecution(FLOW_NAME, FLOW_GROUP, 1234L);
    JobStatus jobStatus = jobStatusIterator.next();
    Assert.assertEquals(jobStatus.getEventName(), ExecutionStatus.COMPLETE.name());
    Assert.assertEquals(jobStatus.getStartTime(), JOB_START_TIME);
    Assert.assertEquals(jobStatus.getEndTime(), JOB_END_TIME);
  }

  @Override
  void cleanUpDir() throws Exception {
    this.dbJobStateStore.delete(KafkaJobStatusMonitor.jobStatusStoreName(FLOW_GROUP, FLOW_NAME));
  }
}
