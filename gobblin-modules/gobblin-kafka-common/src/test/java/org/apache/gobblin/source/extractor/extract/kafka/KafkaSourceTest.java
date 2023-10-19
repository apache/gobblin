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

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.extractor.extract.kafka.validator.TopicNameValidator;
import org.apache.gobblin.source.extractor.extract.kafka.validator.TopicValidators;
import org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaWorkUnitPacker;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.DatasetFilterUtils;

import static org.apache.gobblin.source.extractor.extract.kafka.KafkaSource.*;


public class KafkaSourceTest {
  private static List<String> testTopics =Arrays.asList(
      "topic1", "topic2", "topic3");

  @Test
  public void testGetWorkunits() {
    TestKafkaClient testKafkaClient = new TestKafkaClient();
    testKafkaClient.testTopics = testTopics;
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, "TestPath");
    state.setProp(KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_TYPE, KafkaWorkUnitPacker.PackerType.CUSTOM);
    state.setProp(KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_CUSTOMIZED_TYPE, "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker");
    state.setProp(GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, "MockTestKafkaConsumerClientFactory");
    TestKafkaSource testKafkaSource = new TestKafkaSource(testKafkaClient);
    List<WorkUnit> workUnits = testKafkaSource.getWorkunits(state);

    validatePartitionNumWithinWorkUnits(workUnits, 48);

  }

  @Test
  public void testGetWorkunitsForFilteredPartitions() {
    TestKafkaClient testKafkaClient = new TestKafkaClient();
    List<String> allTopics = testTopics;
    Map<String, List<Integer>> filteredTopicPartitionMap = new HashMap<>();
    filteredTopicPartitionMap.put(allTopics.get(0), new LinkedList<>());
    filteredTopicPartitionMap.put(allTopics.get(1), new LinkedList<>());
    filteredTopicPartitionMap.put(allTopics.get(2), new LinkedList<>());
    filteredTopicPartitionMap.get(allTopics.get(0)).addAll(Arrays.asList(0, 11));
    filteredTopicPartitionMap.get(allTopics.get(1)).addAll(Arrays.asList(2, 8, 10));
    filteredTopicPartitionMap.get(allTopics.get(2)).addAll(Arrays.asList(1, 3, 5, 7));

    testKafkaClient.testTopics = allTopics;
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, "TestPath");
    state.setProp(GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS, "MockTestKafkaConsumerClientFactory");
    TestKafkaSource testKafkaSource = new TestKafkaSource(testKafkaClient);
    List<WorkUnit> workUnits = testKafkaSource.getWorkunitsForFilteredPartitions(state, Optional.of(filteredTopicPartitionMap), Optional.of(3));
    validatePartitionNumWithinWorkUnits(workUnits, 9);

    state.setProp(KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_TYPE, KafkaWorkUnitPacker.PackerType.CUSTOM);
    state.setProp(KafkaWorkUnitPacker.KAFKA_WORKUNIT_PACKER_CUSTOMIZED_TYPE, "org.apache.gobblin.source.extractor.extract.kafka.workunit.packer.KafkaTopicGroupingWorkUnitPacker");
    workUnits = testKafkaSource.getWorkunitsForFilteredPartitions(state, Optional.of(filteredTopicPartitionMap), Optional.of(1));
    validatePartitionNumWithinWorkUnits(workUnits, 9);
  }

  @Test
  public void testGetFilteredTopics() {
    TestKafkaClient testKafkaClient = new TestKafkaClient();
    List<String> allTopics = Arrays.asList(
        "Topic1", "topic-v2", "topic3", // allowed
        "topic-with.period-in_middle", ".topic-with-period-at-start", "topicWithPeriodAtEnd.", //period topics
        "not-allowed-topic");
    testKafkaClient.testTopics = allTopics;

    SourceState state = new SourceState();
    state.setProp(KafkaSource.TOPIC_WHITELIST, ".*[Tt]opic.*");
    state.setProp(KafkaSource.TOPIC_BLACKLIST, "not-allowed.*");
    Assert.assertEquals(new TestKafkaSource(testKafkaClient).getFilteredTopics(state), toKafkaTopicList(allTopics.subList(0, 6)));

    state.setProp(KafkaSource.ALLOW_PERIOD_IN_TOPIC_NAME, false);
    Assert.assertEquals(new TestKafkaSource(testKafkaClient).getFilteredTopics(state), toKafkaTopicList(allTopics.subList(0, 3)));
  }

  @Test
  public void testTopicValidators() {
    TestKafkaClient testKafkaClient = new TestKafkaClient();
    List<String> allTopics = Arrays.asList(
        "Topic1", "topic-v2", "topic3", // allowed
        "topic-with.period-in_middle", ".topic-with-period-at-start", "topicWithPeriodAtEnd.", //period topics
        "not-allowed-topic");
    testKafkaClient.testTopics = allTopics;
    KafkaSource kafkaSource = new TestKafkaSource(testKafkaClient);

    SourceState state = new SourceState();
    state.setProp(KafkaSource.TOPIC_WHITELIST, ".*[Tt]opic.*");
    state.setProp(KafkaSource.TOPIC_BLACKLIST, "not-allowed.*");
    List<KafkaTopic> topicsToValidate = kafkaSource.getFilteredTopics(state);

    // Test without TopicValidators in the state
    Assert.assertTrue(CollectionUtils.isEqualCollection(kafkaSource.getValidTopics(topicsToValidate, state),
        toKafkaTopicList(allTopics.subList(0, 6))));

    // Test empty TopicValidators in the state
    state.setProp(TopicValidators.VALIDATOR_CLASSES_KEY, "");
    Assert.assertTrue(CollectionUtils.isEqualCollection(kafkaSource.getValidTopics(topicsToValidate, state),
        toKafkaTopicList(allTopics.subList(0, 6))));

    // Test TopicValidators with TopicNameValidator in the state
    state.setProp(TopicValidators.VALIDATOR_CLASSES_KEY, TopicNameValidator.class.getName());
    Assert.assertTrue(CollectionUtils.isEqualCollection(kafkaSource.getValidTopics(topicsToValidate, state),
        toKafkaTopicList(allTopics.subList(0, 3))));
  }

  public static List<KafkaPartition> creatPartitions(String topicName, int partitionNum) {
    List<KafkaPartition> partitions = new ArrayList<>(partitionNum);
    for(int i = 0; i < partitionNum; i++ ) {
      partitions.add(new KafkaPartition.Builder().withTopicName(topicName).withId(i).withLeaderHostAndPort("test").withLeaderId(1).build());
    }
    return partitions;
  }

  public static List<KafkaPartition> getPartitionFromWorkUnit(WorkUnit workUnit) {
    List<KafkaPartition> topicPartitions = new ArrayList<>();
    if(workUnit instanceof MultiWorkUnit) {
      for(WorkUnit wu : ((MultiWorkUnit) workUnit).getWorkUnits()) {
        topicPartitions.addAll(getPartitionFromWorkUnit(wu));
      }
    }else {
      int i = 0;
      String partitionIdProp = KafkaSource.PARTITION_ID + "." + i;
      while (workUnit.getProp(partitionIdProp) != null) {
        int partitionId = workUnit.getPropAsInt(partitionIdProp);
        KafkaPartition topicPartition =
            new KafkaPartition.Builder().withTopicName(workUnit.getProp(KafkaSource.TOPIC_NAME)).withId(partitionId).build();
        topicPartitions.add(topicPartition);
        i++;
        partitionIdProp = KafkaSource.PARTITION_ID + "." + i;
      }
    }
    return topicPartitions;
  }


  public static List<KafkaTopic> toKafkaTopicList(List<String> topicNames) {
    return topicNames.stream().map(topicName -> new KafkaTopic(topicName, creatPartitions(topicName, 16))).collect(Collectors.toList());
  }

  private void validatePartitionNumWithinWorkUnits(List<WorkUnit> workUnits, int expectPartitionNum) {
    List<KafkaPartition> partitionList = new ArrayList<>();
    for(WorkUnit workUnit : workUnits) {
      partitionList.addAll(getPartitionFromWorkUnit(workUnit));
    }
    Assert.assertEquals(partitionList.size(), expectPartitionNum);
  }

  @Alias("MockTestKafkaConsumerClientFactory")
  public static class MockTestKafkaConsumerClientFactory
      implements GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory {

    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      return new TestKafkaClient();
    }
  }

  public static class TestKafkaClient implements GobblinKafkaConsumerClient {
    List<String> testTopics = KafkaSourceTest.testTopics;

    @Override
    public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist) {
      return toKafkaTopicList(DatasetFilterUtils.filter(testTopics, blacklist, whitelist));
    }

    @Override
    public long getEarliestOffset(KafkaPartition partition)
        throws KafkaOffsetRetrievalFailureException {
      return 0;
    }

    @Override
    public long getLatestOffset(KafkaPartition partition)
        throws KafkaOffsetRetrievalFailureException {
      return 0;
    }

    @Override
    public Iterator<KafkaConsumerRecord> consume(KafkaPartition partition, long nextOffset, long maxOffset) {
      return null;
    }

    @Override
    public void close()
        throws IOException {

    }
  }

  private class TestKafkaSource<S,D> extends KafkaSource<S,D> {

    public TestKafkaSource(GobblinKafkaConsumerClient client) {
      kafkaConsumerClient.set(client);
    }

    @Override
    public Extractor getExtractor(WorkUnitState state)
        throws IOException {
      throw new RuntimeException("Not implemented");
    }
  }
}
