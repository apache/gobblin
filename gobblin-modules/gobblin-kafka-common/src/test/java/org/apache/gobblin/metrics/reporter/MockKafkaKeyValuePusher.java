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

package org.apache.gobblin.metrics.reporter;

import com.google.common.collect.Queues;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gobblin.metrics.kafka.Pusher;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * Mock instance of {@link org.apache.gobblin.metrics.kafka.Pusher} used to test {@link org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter}.
 */
public class MockKafkaKeyValuePusher<K, V> implements Pusher<Pair<K, V>> {

  Queue<Pair<K, V>> messages = Queues.newLinkedBlockingQueue();

  @Override
  public void pushMessages(List<Pair<K, V>> messages) {
    this.messages.clear();
    this.messages.addAll(messages);
  }

  @Override
  public void close() throws IOException {
  }

  public Iterator<Pair<K, V>> messageIterator() {
    return this.messages.iterator();
  }
}
