package org.apache.gobblin.source.extractor.extract.kafka;

import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaUtilsTest {

  @Test
  public void testGetTopicNameFromTopicPartition() {
    Assert.assertEquals(KafkaUtils.getTopicNameFromTopicPartition("topic-1"), "topic");
    Assert.assertEquals(KafkaUtils.getTopicNameFromTopicPartition("topic-foo-bar-1"), "topic-foo-bar");
  }
}