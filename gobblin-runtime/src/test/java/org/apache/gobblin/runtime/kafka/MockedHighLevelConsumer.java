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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.kafka.client.KafkaConsumerRecord;
import org.apache.gobblin.kafka.client.MockBatchKafkaConsumerClient;
import org.apache.gobblin.testing.AssertWithBackoff;

import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class MockedHighLevelConsumer extends HighLevelConsumer<byte[], byte[]> {

  @Getter
  private final List<byte[]> messages;

  public MockedHighLevelConsumer(String topic, Config config, int numThreads, List<KafkaConsumerRecord> records, int consumeBatchSize) {
    super(topic, config, numThreads, Optional.of(new MockBatchKafkaConsumerClient(config, records, consumeBatchSize)));
    this.messages = Lists.newArrayList();
  }

  public void awaitExactlyNMessages(final int n, int timeoutMillis) throws Exception {
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return MockedHighLevelConsumer.this.messages.size() == n;
      }
    }, timeoutMillis, n + " messages", log, 2, 1000);
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord<byte[], byte[]> message) {
    this.messages.add(message.getValue());
  }

  @Override
  public void shutDown() {
    super.shutDown();
  }
}
