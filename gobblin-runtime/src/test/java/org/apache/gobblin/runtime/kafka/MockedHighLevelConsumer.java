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

import org.mockito.Mockito;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.job_monitor.MockKafkaStream;
import org.apache.gobblin.testing.AssertWithBackoff;

import javax.annotation.Nullable;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
class MockedHighLevelConsumer extends HighLevelConsumer<byte[], byte[]> {
  private final MockKafkaStream mockKafkaStream;
  @Getter
  private final List<String> messages;

  public MockedHighLevelConsumer(Config config, int numThreads, MockKafkaStream stream) {
    super("topic", config, numThreads);

    this.mockKafkaStream = stream;
    this.messages = Lists.newArrayList();
  }

  public void awaitAtLeastNMessages(final int n) throws Exception {
    AssertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return MockedHighLevelConsumer.this.messages.size() >= n;
      }
    }, 1000, n + " messages", log, 2, 1000);
  }

  @Override
  protected void processMessage(MessageAndMetadata<byte[], byte[]> message) {
    this.messages.add(new String(message.message(), Charsets.UTF_8));
  }

  @Override
  protected List<KafkaStream<byte[], byte[]>> createStreams() {
    return this.mockKafkaStream.getMockStreams();
  }

  @Override
  protected ConsumerConnector createConsumerConnector() {
    return Mockito.mock(ConsumerConnector.class);
  }

  @Override
  public void shutDown() {
    this.mockKafkaStream.shutdown();
    super.shutDown();
  }
}
