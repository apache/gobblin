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

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Lists;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;


/**
 * Tests for {@link KafkaSource}
 */
@Test(groups = { "gobblin.source.extractor.extract.kafka" })
public class KafkaSourceTest {

  @Test
  public void testGetTopicSpecificState() {
    String topicName1 = "testTopic1";
    String topicName2 = "testTopic2";
    String topicName3 = "testTopic3";

    KafkaTopic topic1 = createDummyKafkaTopic(topicName1);
    KafkaTopic topic2 = createDummyKafkaTopic(topicName2);
    KafkaTopic topic3 = createDummyKafkaTopic(topicName3);

    String testKey1 = "testKey1";
    String testValue1 = "testValue1";

    SourceState state = new SourceState();
    state.setProp(KafkaSource.KAFKA_TOPIC_SPECIFIC_STATE,
        "[{\"topic.name\" : \"" + topicName1 + "\", \"" + testKey1 + "\" : \"" + testValue1
            + "\"}, {\"topic.name\" : \"" + topicName2 + "\", \"" + testKey1 + "\" : \"" + testValue1 + "\"}]");

    KafkaSource<?, ?> dummyKafkaSource = new KafkaDummySource();
    Map<String, State> topicSpecificStateMap =
        dummyKafkaSource.getTopicSpecificState(Lists.newArrayList(topic1, topic3), state);

    State topic1ExpectedState = new State();
    topic1ExpectedState.setProp(testKey1, testValue1);

    Assert.assertEquals(topicSpecificStateMap.get(topic1.getName()), topic1ExpectedState);
    Assert.assertNull(topicSpecificStateMap.get(topic2.getName()));
    Assert.assertNull(topicSpecificStateMap.get(topic3.getName()));
  }

  @Test
  public void testGetTopicSpecificStateWithRegex() {
    String topicName1 = "testTopic1";
    String topicName2 = "testTopic2";
    String topicName3 = "otherTestTopic1";

    KafkaTopic topic1 = createDummyKafkaTopic(topicName1);
    KafkaTopic topic2 = createDummyKafkaTopic(topicName2);
    KafkaTopic topic3 = createDummyKafkaTopic(topicName3);

    String testKey1 = "testKey1";
    String testValue1 = "testValue1";

    SourceState state = new SourceState();
    state.setProp(KafkaSource.KAFKA_TOPIC_SPECIFIC_STATE,
        "[{\"topic.name\" : \"testTopic.*\", \"" + testKey1 + "\" : \"" + testValue1 + "\"}]");

    KafkaSource<?, ?> dummyKafkaSource = new KafkaDummySource();
    Map<String, State> topicSpecificStateMap =
        dummyKafkaSource.getTopicSpecificState(Lists.newArrayList(topic1, topic2, topic3), state);

    State topic1ExpectedState = new State();
    topic1ExpectedState.setProp(testKey1, testValue1);

    State topic2ExpectedState = new State();
    topic2ExpectedState.setProp(testKey1, testValue1);

    Assert.assertEquals(topicSpecificStateMap.get(topic1.getName()), topic1ExpectedState);
    Assert.assertEquals(topicSpecificStateMap.get(topic2.getName()), topic2ExpectedState);
    Assert.assertNull(topicSpecificStateMap.get(topic3.getName()));
  }

  private KafkaTopic createDummyKafkaTopic(String topicName) {
    KafkaPartition partition = new KafkaPartition.Builder().withTopicName(topicName).withId(1)
        .withLeaderHostAndPort("testHost", 1).withLeaderId(1).build();

    return new KafkaTopic(topicName, Lists.newArrayList(partition));
  }

  private static class KafkaDummySource extends KafkaSource {

    @Override
    public Extractor getExtractor(WorkUnitState state) throws IOException {
      return null;
    }
  }
}
