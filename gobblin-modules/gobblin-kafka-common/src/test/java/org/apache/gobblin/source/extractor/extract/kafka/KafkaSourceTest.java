package org.apache.gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.util.DatasetFilterUtils;


public class KafkaSourceTest {

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

  public List<KafkaTopic> toKafkaTopicList(List<String> topicNames) {
    return topicNames.stream().map(topicName -> new KafkaTopic(topicName, Collections.emptyList())).collect(Collectors.toList());
  }

  private class TestKafkaClient implements GobblinKafkaConsumerClient {
    List<String> testTopics;

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