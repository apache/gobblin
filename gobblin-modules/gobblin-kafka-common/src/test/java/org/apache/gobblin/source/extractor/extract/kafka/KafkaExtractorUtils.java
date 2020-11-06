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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.File;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.metastore.FileContextBasedFsStateStoreFactory;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.runtime.StateStoreBasedWatermarkStorage;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker;

import static org.apache.gobblin.configuration.ConfigurationKeys.JOB_NAME_KEY;
import static org.apache.gobblin.configuration.ConfigurationKeys.WATERMARK_INTERVAL_VALUE_KEY;

@Slf4j
public class KafkaExtractorUtils {
  private static final Integer MAX_NUM_BROKERS = 100;
  private static final Integer MAX_NUM_TOPIC_PARTITIONS = 1024;

  /**
   * A utility method that returns a {@link WorkUnitState} which can be used to instantiate both a batch and a
   * streaming Kafka extractor.
   * @param topicName
   * @param numPartitions
   * @return
   */
  public static WorkUnitState getWorkUnitState(String topicName, int numPartitions) {
    Preconditions.checkArgument(numPartitions <= MAX_NUM_TOPIC_PARTITIONS, "Num partitions assigned"
        + "must be smaller than the maximum number of partitions of the topic");
    WorkUnitState state = new WorkUnitState();
    state.setProp(JOB_NAME_KEY, "testJob");
    state.setProp(KafkaSource.TOPIC_NAME, topicName);
    state.setProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, KafkaStreamTestUtils.MockKafka10ConsumerClientFactory.class.getName());
    state.setProp(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, KafkaStreamTestUtils.MockSchemaRegistry.class.getName());
    //Need to set this property for LiKafka10ConsumerClient instantiation
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL, "http://dummySchemaRegistry:1000");
    Random random = new Random();
    for (int i=0; i<numPartitions; i++) {
      //Assign a random partition id.
      state.setProp(KafkaSource.PARTITION_ID + "." + i, random.nextInt(MAX_NUM_TOPIC_PARTITIONS));
      state.setProp(KafkaSource.LEADER_ID + "." + i, random.nextInt(MAX_NUM_BROKERS));
      state.setProp(KafkaSource.LEADER_HOSTANDPORT + "." + i, "leader-" + i + ":9091");
    }
    state.setProp(KafkaTopicGroupingWorkUnitPacker.NUM_PARTITIONS_ASSIGNED, numPartitions);
    //Configure the watermark storage. We use FileContextBasedFsStateStoreFactory, since it allows for overwriting an existing
    // state store.
    state.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, FileContextBasedFsStateStoreFactory.class.getName());
    File stateStoreDir = Files.createTempDir();
    stateStoreDir.deleteOnExit();
    state.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, stateStoreDir.getAbsolutePath());

    //Kafka configurations
    state.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:9091");

    // Constructing a dummy watermark and mock client-factory-class and registry-class
    List<Long> dummyWatermark = ImmutableList.of(new Long(1));
    WatermarkInterval watermarkInterval =
        new WatermarkInterval(new MultiLongWatermark(dummyWatermark), new MultiLongWatermark(dummyWatermark));
    state.setProp(WATERMARK_INTERVAL_VALUE_KEY, watermarkInterval.toJson());

    state.setWuProperties(state.getProperties(), state.getProperties());
    return state;
  }
}