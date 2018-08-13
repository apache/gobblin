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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.kafka.HighLevelConsumerTest;
import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class AvroJobSpecKafkaJobMonitorTest {

  @Test
  public void test() throws Exception {

    Config config = HighLevelConsumerTest.getSimpleConfig(Optional.of(KafkaJobMonitor.KAFKA_JOB_MONITOR_PREFIX));
    String stateStoreRootDir = config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
    FileSystem fs = FileSystem.getLocal(new Configuration());

    MockedAvroJobSpecKafkaJobMonitor monitor = MockedAvroJobSpecKafkaJobMonitor.create(config);
    monitor.startAsync();

    monitor.getMockKafkaStream().pushToStream("/flow1/job1:1");
    monitor.awaitExactlyNSpecs(1);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("/flow1/job1")));

    monitor.getMockKafkaStream().pushToStream("/flow2/job2:1");
    monitor.awaitExactlyNSpecs(2);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("/flow2/job2")));

    monitor.getMockKafkaStream().pushToStream("/flow3/job3:1");
    monitor.awaitExactlyNSpecs(3);
    Assert.assertTrue(monitor.getJobSpecs().containsKey(new URI("/flow3/job3")));

    Path statestore = new Path(stateStoreRootDir, "job1");
    fs.create(statestore);
    new File(statestore.toString()).deleteOnExit();
    Assert.assertTrue(fs.exists(new Path(stateStoreRootDir, "job1")));

    monitor.getMockKafkaStream().pushToStream(MockedAvroJobSpecKafkaJobMonitor.REMOVE + ":/flow1/job1");
    monitor.awaitExactlyNSpecs(2);
    Assert.assertFalse(monitor.getJobSpecs().containsKey(new URI("/flow1/job1")));
    Assert.assertTrue(fs.exists(new Path(stateStoreRootDir, "job1")));

    statestore = new Path(stateStoreRootDir, "job2");
    fs.create(statestore);
    new File(statestore.toString()).deleteOnExit();

    monitor.getMockKafkaStream().pushToStream(MockedAvroJobSpecKafkaJobMonitor.REMOVE_WITH_STATE + ":/flow2/job2");
    monitor.awaitExactlyNSpecs(1);
    Assert.assertFalse(monitor.getJobSpecs().containsKey(new URI("/flow2/job2")));
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
                                   @Override
                                   public boolean apply(@Nullable Void input) {
                                     try {
                                       return !fs.exists(new Path(stateStoreRootDir, "job2"));
                                     } catch (IOException e) {
                                     }
                                     return false;
                                   }
                                 },
        100, "Statestore not deleted.", log, 2, 2000
    );

    monitor.shutDown();
  }
}
