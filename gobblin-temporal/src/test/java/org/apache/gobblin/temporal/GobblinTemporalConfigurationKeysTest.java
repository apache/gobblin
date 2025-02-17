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

package org.apache.gobblin.temporal;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.config.ConfigBuilder;



public class GobblinTemporalConfigurationKeysTest {

  @Test
  public void testGetTemporalTaskQueueName_DefaultConfig() {
    // Empty configuration
    Config config = ConfigFactory.empty();

    // Call the function to get the queue name
    String queueName = GobblinTemporalConfigurationKeys.getTemporalTaskQueueName(config);

    Assert.assertTrue(queueName.startsWith(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE));

    // Extract and validate UUID part
    String uuidPart =
        queueName.substring(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE.length());
    UUID uuid = UUID.fromString(uuidPart);
    Assert.assertNotNull(uuid);
  }

  @Test
  public void testGetTemporalTaskQueueName_CustomConfig() {
    // Custom task queue prefix
    Config config = ConfigFactory.parseString("gobblin.temporal.task.queue.name=myCustomQueue");

    // Call the function to get the queue name
    String queueName = GobblinTemporalConfigurationKeys.getTemporalTaskQueueName(config);

    Assert.assertTrue(queueName.startsWith("myCustomQueue"));

    // Extract and validate UUID part
    String uuidPart = queueName.substring("myCustomQueue".length());
    UUID uuid = UUID.fromString(uuidPart);
    Assert.assertNotNull(uuid);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testGetTemporalTaskQueueName_NullConfig() {
    // Null configuration
    Config config = null;

    // Call the function to get the queue name
    String queueName = GobblinTemporalConfigurationKeys.getTemporalTaskQueueName(config);
  }

  @Test
  public void testGetTemporalTaskQueueName_WithConfigBuilder() {
    // Simulate job properties
    Properties jobProps = new Properties();
    jobProps.put(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE, "customTaskQueue");


    // Build configuration using ConfigBuilder
    Config config = ConfigBuilder.create()
        .addPrimitive(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE, jobProps.getProperty(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE))
        .build();

    // Call the function to get the queue name
    String queueName = GobblinTemporalConfigurationKeys.getTemporalTaskQueueName(config);

    // Assert that the queue name starts with the correct prefix value
    Assert.assertTrue(queueName.startsWith("customTaskQueue"));

    // Extract and validate UUID part
    String uuidPart = queueName.substring("customTaskQueue".length());
    UUID uuid = UUID.fromString(uuidPart);
    Assert.assertNotNull(uuid);
  }

  @Test
  public void testGetTemporalTaskQueueName_MultipleCalls() {
    // Configuration
    Config config = ConfigFactory.empty();

    // Set to store generated queue names
    Set<String> queueNames = new HashSet<>();

    // Generate multiple queue names
    for (int i = 0; i < 100; i++) {
      String queueName = GobblinTemporalConfigurationKeys.getTemporalTaskQueueName(config);
      Assert.assertTrue(queueName.startsWith(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE));

      // Ensure uniqueness
      Assert.assertFalse(queueNames.contains(queueName));

      // Store the queue name for future comparisons
      queueNames.add(queueName);

      // Extract and validate UUID part
      String uuidPart =
          queueName.substring(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE.length());
      UUID uuid = UUID.fromString(uuidPart);
      Assert.assertNotNull(uuid);
    }
  }
}
