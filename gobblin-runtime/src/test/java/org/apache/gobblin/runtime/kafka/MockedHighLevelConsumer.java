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

package org.apache.gobblin.runtime.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.testing.AssertWithBackoff;

@Slf4j
public class MockedHighLevelConsumer extends HighLevelConsumer<byte[], byte[]> {

  @Getter
  private final List<byte[]> messages;
  @Getter
  private final Map<KafkaPartition, Long> committedOffsets;

  public MockedHighLevelConsumer(String topic, Config config, int numThreads) {
    super(topic, config, numThreads);
    this.messages = Lists.newArrayList();
    this.committedOffsets = new ConcurrentHashMap<>();
  }

  public void awaitExactlyNMessages(final int n, int timeoutMillis) throws Exception {
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return MockedHighLevelConsumer.this.messages.size() == n;
      }
    }, timeoutMillis, "Expected: " + n + " messages, consumed: " + this.messages.size() , log, 2, 1000);
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord<byte[], byte[]> message) {
    this.messages.add(message.getValue());
  }

  @Override
  protected void commitOffsets(Map<KafkaPartition, Long> partitionOffsets) {
    super.commitOffsets(partitionOffsets);
    committedOffsets.putAll(partitionOffsets.entrySet().stream().collect(Collectors
        .toMap(e -> e.getKey(), e -> e.getValue())));
  }

  @Override
  public void shutDown() {
    super.shutDown();
  }
}
