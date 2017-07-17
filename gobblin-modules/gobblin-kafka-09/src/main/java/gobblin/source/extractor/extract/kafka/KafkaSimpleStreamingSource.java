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

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.stream.RecordEnvelope;
import gobblin.source.extractor.extract.EventBasedSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ConfigUtils;


/**
 * A {@link EventBasedSource} implementation for a simple streaming kafka extractor.
 *
 * @author Shrikanth Shankar
 *
 */
public class KafkaSimpleStreamingSource<S, D> extends EventBasedSource<S, RecordEnvelope<D>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleStreamingSource.class);

  /**
   * This is the topic name used by this source . Currently only supports singleton.
   */
  public static final String TOPIC_WHITELIST = "gobblin.streaming.kafka.topic.singleton";
  /**
   * Deserializer to be used by this source.
   */
  public static final String TOPIC_KEY_DESERIALIZER = "gobblin.streaming.kafka.topic.key.deserializer";
  public static final String TOPIC_VALUE_DESERIALIZER = "gobblin.streaming.kafka.topic.value.deserializer";

  /**
   * Private config keys used to pass data into work unit state
   */
  private static final String TOPIC_NAME = KafkaSource.TOPIC_NAME;
  private static final String PARTITION_ID = KafkaSource.PARTITION_ID;

  public static String getTopicNameFromState(State s) {
    return s.getProp(TOPIC_NAME);
  }

  public static int getPartitionIdFromState(State s) {
    return s.getPropAsInt(PARTITION_ID);
  }

  public static void setTopicNameInState(State s, String topic) {
    s.setProp(TOPIC_NAME, topic);
  }

  public static void setPartitionId(State s, int partitionId) {
    s.setProp(PARTITION_ID, partitionId);
  }

  private final Closer closer = Closer.create();
  public static final Extract.TableType DEFAULT_TABLE_TYPE = Extract.TableType.APPEND_ONLY;
  public static final String DEFAULT_NAMESPACE_NAME = "KAFKA";

  static public Consumer getKafkaConsumer(Config config) {
    List<String> brokers = ConfigUtils.getStringList(config, ConfigurationKeys.KAFKA_BROKERS);
    Properties props = new Properties();
    props.put("bootstrap.servers", Joiner.on(",").join(brokers));
    props.put("group.id", ConfigUtils.getString(config, ConfigurationKeys.JOB_NAME_KEY, StringUtils.EMPTY));
    props.put("enable.auto.commit", "false");
    Preconditions.checkArgument(config.hasPath(TOPIC_KEY_DESERIALIZER));
    props.put("key.deserializer", config.getString(TOPIC_KEY_DESERIALIZER));
    Preconditions.checkArgument(config.hasPath(TOPIC_VALUE_DESERIALIZER));
    props.put("value.deserializer", config.getString(TOPIC_VALUE_DESERIALIZER));
    Consumer consumer = null;
    try {
      consumer = new KafkaConsumer<>(props);
    } catch (Exception e) {
      LOG.error("Exception when creating Kafka consumer - {}", e);
      throw Throwables.propagate(e);
    }
    return consumer;
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Config config = ConfigUtils.propertiesToConfig(state.getProperties());
    Consumer<String, byte[]> consumer = getKafkaConsumer(config);
    LOG.debug("Consumer is {}", consumer);
    String topic = ConfigUtils.getString(config, TOPIC_WHITELIST,
        StringUtils.EMPTY); // TODO: fix this to use the new API when KafkaWrapper is fixed
    List<WorkUnit> workUnits = new ArrayList<WorkUnit>();
    List<PartitionInfo> topicPartitions;
    topicPartitions = consumer.partitionsFor(topic);
    LOG.info("Partition count is {}", topicPartitions.size());
    for (PartitionInfo topicPartition : topicPartitions) {
      Extract extract = this.createExtract(DEFAULT_TABLE_TYPE, DEFAULT_NAMESPACE_NAME, topicPartition.topic());
      LOG.info("Partition info is {}", topicPartition);
      WorkUnit workUnit = WorkUnit.create(extract);
      setTopicNameInState(workUnit, topicPartition.topic());
      workUnit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, topicPartition.topic());
      setPartitionId(workUnit, topicPartition.partition());
      workUnits.add(workUnit);
    }
    return workUnits;
  }


  @Override
  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
    return new KafkaSimpleStreamingExtractor<S, D>(state);
  }
}
