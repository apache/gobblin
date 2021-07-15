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
package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaUtils;
import org.apache.gobblin.source.extractor.extract.kafka.UniversalKafkaSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class KafkaTopicGroupingWorkUnitPackerTest {
  private Properties props;

  @BeforeMethod
  public void setUp() {
    props = new Properties();
    props.setProperty("gobblin.kafka.streaming.containerCapacity", "2");
    props.setProperty("kafka.workunit.size.estimator.type", "CUSTOM");
    props.setProperty("kafka.workunit.size.estimator.customizedType",
        "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.UnitKafkaWorkUnitSizeEstimator");
  }

  /**
   * Check that capacity is honored.
   */
  @Test
  public void testSingleTopic() {
    KafkaSource source = new UniversalKafkaSource();
    SourceState state = new SourceState(new State(props));
    state.setProp("gobblin.kafka.streaming.enableIndexing", false);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, Files.createTempDir().getAbsolutePath());

    Map<String, List<WorkUnit>> workUnitsByTopic = ImmutableMap.of("topic1", Lists
        .newArrayList(getWorkUnitWithTopicPartition("topic1", 1), getWorkUnitWithTopicPartition("topic1", 2),
            getWorkUnitWithTopicPartition("topic1", 3)));

    List<WorkUnit> workUnits = new KafkaTopicGroupingWorkUnitPacker(source, state, Optional.absent()).pack(workUnitsByTopic, 10);
    Assert.assertEquals(workUnits.size(), 2);
    Assert.assertEquals(workUnits.get(0).getProp(KafkaSource.TOPIC_NAME), "topic1");
    Assert.assertEquals(workUnits.get(0).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 1);
    Assert.assertEquals(workUnits.get(0).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 1)), 2);
    Assert.assertEquals(workUnits.get(0).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);
    Assert.assertEquals(workUnits.get(1).getProp(KafkaSource.TOPIC_NAME), "topic1");
    Assert.assertEquals(workUnits.get(1).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 3);
    Assert.assertEquals(workUnits.get(1).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);
  }

  /**
   * Check that topics are kept in separate work units.
   */
  @Test
  public void testMultiTopic() {
    KafkaSource source = new UniversalKafkaSource();
    SourceState state = new SourceState(new State(props));
    state.setProp("gobblin.kafka.streaming.enableIndexing", false);
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, Files.createTempDir().getAbsolutePath());

    Map<String, List<WorkUnit>> workUnitsByTopic = ImmutableMap.of("topic1", Lists
        .newArrayList(getWorkUnitWithTopicPartition("topic1", 1), getWorkUnitWithTopicPartition("topic1", 2),
            getWorkUnitWithTopicPartition("topic1", 3)), "topic2", Lists
        .newArrayList(getWorkUnitWithTopicPartition("topic2", 1), getWorkUnitWithTopicPartition("topic2", 2),
            getWorkUnitWithTopicPartition("topic2", 3)));

    List<WorkUnit> workUnits = new KafkaTopicGroupingWorkUnitPacker(source, state, Optional.absent()).pack(workUnitsByTopic, 10);
    Assert.assertEquals(workUnits.size(), 4);

    Assert.assertEquals(workUnits.get(0).getProp(KafkaSource.TOPIC_NAME), "topic1");
    Assert.assertEquals(workUnits.get(0).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 1);
    Assert.assertEquals(workUnits.get(0).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 1)), 2);
    Assert.assertEquals(workUnits.get(0).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);

    Assert.assertEquals(workUnits.get(1).getProp(KafkaSource.TOPIC_NAME), "topic1");
    Assert.assertEquals(workUnits.get(1).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 3);
    Assert.assertEquals(workUnits.get(1).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);

    Assert.assertEquals(workUnits.get(2).getProp(KafkaSource.TOPIC_NAME), "topic2");
    Assert.assertEquals(workUnits.get(2).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 1);
    Assert.assertEquals(workUnits.get(2).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 1)), 2);
    Assert.assertEquals(workUnits.get(2).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);

    Assert.assertEquals(workUnits.get(3).getProp(KafkaSource.TOPIC_NAME), "topic2");
    Assert.assertEquals(workUnits.get(3).getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, 0)), 3);
    Assert.assertEquals(workUnits.get(3).getPropAsDouble(KafkaTopicGroupingWorkUnitPacker.CONTAINER_CAPACITY_KEY), 2, 0.001);
  }


  public WorkUnit getWorkUnitWithTopicPartition(String topic, int partition) {
    WorkUnit workUnit = new WorkUnit(new Extract(Extract.TableType.APPEND_ONLY, "kafka", topic));
    workUnit.setProp(KafkaSource.TOPIC_NAME, topic);
    workUnit.setProp(KafkaSource.PARTITION_ID, Integer.toString(partition));
    workUnit.setProp(KafkaSource.LEADER_HOSTANDPORT, "host:1234");
    workUnit.setProp(KafkaSource.LEADER_ID, "1");

    workUnit.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, "0");
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, "0");

    workUnit.setProp("previousStartFetchEpochTime", "0");
    workUnit.setProp("previousStopFetchEpochTime", "0");
    workUnit.setProp("previousLowWatermark", "0");
    workUnit.setProp("previousHighWatermark", "0");
    workUnit.setProp("previousLatestOffset", "0");
    workUnit.setProp("offsetFetchEpochTime", "0");
    workUnit.setProp("previousOffsetFetchEpochTime", "0");

    return workUnit;
  }

  @Test
  public void testGetContainerCapacityForTopic() {
    double delta = 0.000001; //Error tolerance limit for assertions involving double.
    KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy strategy =
        KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy.MIN;
    List<Double> capacities = Arrays.asList(new Double[]{1.2, 1.4, 1.3, 1.4, 1.2});
    double capacity = KafkaTopicGroupingWorkUnitPacker.getContainerCapacityForTopic(capacities, strategy);
    Assert.assertEquals(capacity, 1.2, delta);
    strategy = KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy.MAX;
    capacity = KafkaTopicGroupingWorkUnitPacker.getContainerCapacityForTopic(capacities, strategy);
    Assert.assertEquals(capacity, 1.4, delta);
    strategy = KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy.MEDIAN;
    capacity = KafkaTopicGroupingWorkUnitPacker.getContainerCapacityForTopic(capacities, strategy);
    Assert.assertEquals(capacity, 1.3, delta);
    strategy = KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy.MEAN;
    capacity = KafkaTopicGroupingWorkUnitPacker.getContainerCapacityForTopic(capacities, strategy);
    Assert.assertEquals(capacity, 1.3, delta);

    //Validate the median for an even sized list
    capacities = Arrays.asList(new Double[]{1.2, 1.4, 1.3, 1.4});
    strategy = KafkaTopicGroupingWorkUnitPacker.ContainerCapacityComputationStrategy.MEDIAN;
    capacity = KafkaTopicGroupingWorkUnitPacker.getContainerCapacityForTopic(capacities, strategy);
    Assert.assertEquals(capacity, 1.35, delta);
  }
}