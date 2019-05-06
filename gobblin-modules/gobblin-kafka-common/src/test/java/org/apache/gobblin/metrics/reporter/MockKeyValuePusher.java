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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Queues;


/**
 * Mock instance of {@link org.apache.gobblin.metrics.reporter.KeyValuePusher} used to test
 * {@link KeyValueMetricObjectReporter}
 * {@link KeyValueEventObjectReporter}
 */

public class MockKeyValuePusher<K, V> implements KeyValuePusher<K, V> {

  Queue<Pair<K, V>> messages = Queues.newLinkedBlockingQueue();

  @Override
  public void pushKeyValueMessages(List<Pair<K, V>> messages) {
    this.messages.clear();
    this.messages.addAll(messages);
  }

  @Override
  public void pushMessages(List<V> messages) {

  }

  @Override
  public void close()
      throws IOException {

  }

  public Iterator<Pair<K, V>> messageIterator() {
    return this.messages.iterator();
  }
}
