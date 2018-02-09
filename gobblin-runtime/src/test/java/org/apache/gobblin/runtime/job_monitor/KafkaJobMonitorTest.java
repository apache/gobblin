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

package org.apache.gobblin.runtime.job_monitor;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.kafka.HighLevelConsumerTest;


public class KafkaJobMonitorTest {

  @Test
  public void test() throws Exception {

    Config config = HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX));
    String stateStoreRootDir = config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
    FileSystem fs = FileSystem.getLocal(new Configuration());

    MockedKafkaJobMonitor monitor = MockedKafkaJobMonitor.create(config);
    monitor.startAsync();

    monitor.getMockKafkaStream().pushToStream("job1:1");
    monitor.awaitExactlyNSpecs(1);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "1");

    monitor.getMockKafkaStream().pushToStream("job2:1");
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "1");

    monitor.getMockKafkaStream().pushToStream(MockedKafkaJobMonitor.REMOVE + ":job1");
    monitor.awaitExactlyNSpecs(1);
    Assert.assertFalse(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));

    monitor.getMockKafkaStream().pushToStream("job2:2,job1:2");
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job1")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job1")).getVersion(), "2");
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("job2")));
    Assert.assertEquals(monitor.getJobSpecs().get(new URI("job2")).getVersion(), "2");

    monitor.getMockKafkaStream().pushToStream("/flow3/job3:1");
    monitor.awaitExactlyNSpecs(3);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("/flow3/job3")));

    // TODO: Currently, state stores are not categorized by flow name.
    //       This can lead to one job overwriting other jobs' job state.
    fs.create(new Path(stateStoreRootDir, "job3"));
    Assert.assertTrue(fs.exists(new Path(stateStoreRootDir, "job3")));
    monitor.getMockKafkaStream().pushToStream(MockedKafkaJobMonitor.REMOVE + ":/flow3/job3");
    monitor.awaitExactlyNSpecs(2);
    Assert.assertFalse(fs.exists(new Path(stateStoreRootDir, "job3")));

    monitor.shutDown();
  }
}
