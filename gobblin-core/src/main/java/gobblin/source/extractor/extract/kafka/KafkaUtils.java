/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import gobblin.configuration.State;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A Utils class for Kafka.
 */
public class KafkaUtils {

  /**
   * Get topic name from a state object. The state should contain property
   * "topic.name".
   */
  public static String getTopicName(State state) {
    Preconditions.checkArgument(state.contains(KafkaSource.TOPIC_NAME));

    return state.getProp(KafkaSource.TOPIC_NAME);
  }

  /**
   * Get kafka partition from a state object. The state should contain properties
   * "topic.name", "partition.id", and may optionally contain "leader.id" and
   * "leader.hostandport".
   */
  public static KafkaPartition getPartition(State state) {
    Preconditions.checkArgument(state.contains(KafkaSource.TOPIC_NAME) && state.contains(KafkaSource.PARTITION_ID));

    KafkaPartition.Builder builder =
        new KafkaPartition.Builder().withTopicName(state.getProp(KafkaSource.TOPIC_NAME)).withId(
            state.getPropAsInt(KafkaSource.PARTITION_ID));
    if (state.contains(KafkaSource.LEADER_ID)) {
      builder = builder.withLeaderId(state.getPropAsInt(KafkaSource.LEADER_ID));
    }
    if (state.contains(KafkaSource.LEADER_HOSTANDPORT)) {
      builder = builder.withLeaderHostAndPort(state.getProp(KafkaSource.LEADER_HOSTANDPORT));
    }
    return builder.build();
  }

  /**
   * Get a list of Kafka partitions from a state object. The state should contain properties
   * "topic.name", "partition.id.i", "leader.id.i" and "leader.hostandport.i", i = 0,1,2,...
   *
   * All partitions should have the same topic name.
   */
  public static List<KafkaPartition> getPartitions(State state) {
    List<KafkaPartition> partitions = Lists.newArrayList();
    String topicName = state.getProp(KafkaSource.TOPIC_NAME);
    for (int i = 0;; i++) {
      if (!state.contains(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i))) {
        break;
      }
      KafkaPartition partition =
          new KafkaPartition.Builder().withTopicName(topicName)
              .withId(state.getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.PARTITION_ID, i)))
              .withLeaderId(state.getPropAsInt(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_ID, i)))
              .withLeaderHostAndPort(state.getProp(KafkaUtils.getPartitionPropName(KafkaSource.LEADER_HOSTANDPORT, i)))
              .build();
      partitions.add(partition);
    }
    return partitions;
  }

  /**
   * This method returns baseName + '.' + idx.
   */
  public static String getPartitionPropName(String baseName, int idx) {
    return baseName + "." + idx;
  }

  /**
   * Get the average event size of a partition, which is stored in property "[topicname].[partitionid].avg.event.size".
   * If state doesn't contain this property, it returns defaultSize.
   */
  public static long getPartitionAvgEventSize(State state, KafkaPartition partition, long defaultSize) {
    return state.getPropAsLong(KafkaSource.TOPIC_NAME + "." + KafkaSource.PARTITION_ID + "."
        + KafkaSource.AVG_EVENT_SIZE, defaultSize);
  }

  /**
   * Set the average event size of a partition, which will be stored in property
   * "[topicname].[partitionid].avg.event.size".
   */
  public static void setPartitionAvgEventSize(State state, KafkaPartition partition, long size) {
    state.setProp(KafkaSource.TOPIC_NAME + "." + KafkaSource.PARTITION_ID + "." + KafkaSource.AVG_EVENT_SIZE, size);
  }

}
