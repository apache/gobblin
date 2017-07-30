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

package gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import com.google.common.collect.Queues;

import kafka.producer.ProducerConfig;

import gobblin.metrics.kafka.KafkaPusher;
import gobblin.metrics.kafka.ProducerCloseable;


/**
 * Mock instance of {@link gobblin.metrics.kafka.KafkaPusher} used for testing.
 */
public class MockKafkaPusher extends KafkaPusher {

  Queue<byte[]> messages = Queues.newLinkedBlockingQueue();

  public MockKafkaPusher() {
    super("dummy", "dummy");
  }

  @Override
  public void pushMessages(List<byte[]> messages) {
    this.messages.addAll(messages);
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }

  @Override
  protected ProducerCloseable<String, byte[]> createProducer(ProducerConfig config) {
    return null;
  }

  public Iterator<byte[]> messageIterator() {
    return this.messages.iterator();
  }

}
