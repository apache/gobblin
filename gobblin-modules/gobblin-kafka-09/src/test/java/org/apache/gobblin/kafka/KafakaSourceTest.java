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

package org.apache.gobblin.kafka;

import com.typesafe.config.Config;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.GobblinKafkaConsumerClient;
import org.apache.gobblin.kafka.client.Kafka09ConsumerClient;
import org.apache.gobblin.runtime.util.MultiWorkUnitUnpackingIterator;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


@Slf4j
public class KafakaSourceTest {

  public static final String BOOTSTRAP_WITH_OFFSET = "bootstrap.with.offset";
  public static final String PARTITION_ID = "partition.id";

  private final KafkaTestBase kafkaTestHelper;
  public KafakaSourceTest()
      throws InterruptedException, RuntimeException {
    kafkaTestHelper = new KafkaTestBase();
  }
  @BeforeSuite
  public void beforeSuite() {
    log.info("Process id = " + ManagementFactory.getRuntimeMXBean().getName());
    kafkaTestHelper.startServers();
  }

  @AfterSuite
  public void afterSuite()
      throws IOException {
    try {
      kafkaTestHelper.stopClients();
    } finally {
      kafkaTestHelper.stopServers();
    }
  }
  private SourceState generateState()
  {
    SourceState state = new SourceState();
    state.setProp(KafkaSource.GOBBLIN_KAFKA_CONSUMER_CLIENT_FACTORY_CLASS,
        TestKafkaConsumerClientFactory.class.getName());
    state.setProp(ConfigurationKeys.KAFKA_BROKERS, "localhost:" + kafkaTestHelper.getKafkaServerPort());
    state.setProp(KafkaSource.TOPIC_WHITELIST, "test_topic.*");
    state.setProp(BOOTSTRAP_WITH_OFFSET, "earliest");
    return state;
  }

  @Test
  public void testWhenNoTargetMapperSize() {

    KafkaSourceForTest source = new KafkaSourceForTest();
    SourceState state = generateState();
    state.setProp(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, 100);
    List<WorkUnit> workUnits =  source.getWorkunits(state);
    Assert.assertEquals(workUnits.size(), 100);

    MultiWorkUnitUnpackingIterator iterator = new MultiWorkUnitUnpackingIterator(workUnits.iterator());
    int numOfWorkUnits = 0;
    while(iterator.hasNext())
    {
      numOfWorkUnits++;
      iterator.next();
    }
    Assert.assertEquals(numOfWorkUnits,3);
  }

  @Test
  public void testWhenTargetMapperSizeSet() {

    KafkaSourceForTest source = new KafkaSourceForTest();
    SourceState state = generateState();
    state.setProp(ConfigurationKeys.MR_TARGET_MAPPER_SIZE, 2000);
    List<WorkUnit> workUnits =  source.getWorkunits(state);
    Assert.assertEquals(workUnits.size(), 2);
    MultiWorkUnit multiWorkUnit = (MultiWorkUnit)workUnits.get(0);
    Assert.assertEquals(multiWorkUnit.getWorkUnits().size(),2);
    multiWorkUnit = (MultiWorkUnit)workUnits.get(1);
    Assert.assertEquals(multiWorkUnit.getWorkUnits().size(),1);
    Assert.assertEquals(multiWorkUnit.getWorkUnits().get(0).getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY), "test_topic");
    Assert.assertEquals(multiWorkUnit.getWorkUnits().get(0).getPropAsInt(PARTITION_ID), 0);
  }

  public class KafkaSourceForTest extends KafkaSource<String, String>
  {
    @Override
    public Extractor<String, String> getExtractor(WorkUnitState state)
    {
      return null;
    }
  }
  /**
   * A factory class to instantiate {@link Kafka09ConsumerClient} using mock consumer for test
   */
  public static class TestKafkaConsumerClientFactory implements GobblinKafkaConsumerClient.GobblinKafkaConsumerClientFactory {
    @SuppressWarnings("rawtypes")
    @Override
    public GobblinKafkaConsumerClient create(Config config) {
      MockConsumer<String, String> consumer = new MockConsumer<String, String>(OffsetResetStrategy.NONE);
      consumer.assign(Arrays.asList(new TopicPartition("test_topic", 0), new TopicPartition("test_topic", 1), new TopicPartition("test_topic1", 0)));

      HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
      beginningOffsets.put(new TopicPartition("test_topic", 0), 0L);
      beginningOffsets.put(new TopicPartition("test_topic", 1), 0L);
      beginningOffsets.put(new TopicPartition("test_topic1", 0), 0L);
      consumer.updateBeginningOffsets(beginningOffsets);
      HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
      endOffsets.put(new TopicPartition("test_topic", 0), 2000L);
      endOffsets.put(new TopicPartition("test_topic", 1), 100L);
      endOffsets.put(new TopicPartition("test_topic1", 0), 200L);
      consumer.updateEndOffsets(endOffsets);

      Node leadForTest = new Node(0,"localhost", 1);
      List<PartitionInfo> partionInfo0 = Arrays.asList(new PartitionInfo("test_topic",0,leadForTest, null,null),new PartitionInfo("test_topic",1,leadForTest,null,null));
      consumer.updatePartitions("test_topic", partionInfo0);
      List<PartitionInfo> partionInfo1 = Arrays.asList(new PartitionInfo("test_topic1",0,leadForTest ,null,null));
      consumer.updatePartitions("test_topic1", partionInfo1);

      return new Kafka09ConsumerClient(config, consumer);
    }
  }
}


