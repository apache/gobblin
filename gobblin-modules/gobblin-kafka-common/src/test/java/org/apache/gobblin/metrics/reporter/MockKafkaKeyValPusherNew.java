package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.Queues;

import org.apache.gobblin.metrics.kafka.KeyValuePusher;


/**
 * Mock instance of {@link org.apache.gobblin.metrics.kafka.KeyValuePusher} used to test
 * {@link org.apache.gobblin.metrics.kafka.KafkaKeyValueMetricObjectReporter}
 * {@link org.apache.gobblin.metrics.kafka.KafkaKeyValueEventObjectReporter}
 * {@link org.apache.gobblin.metrics.kafka.KafkaAvroMetricKeyValueReporter}
 */

public class MockKafkaKeyValPusherNew<K,V> implements KeyValuePusher<K,V> {

  Queue<Pair<K,V>> messages = Queues.newLinkedBlockingQueue();

  @Override
  public void pushKeyValueMessages(List<Pair<K, V>> messages) {
    this.messages.clear();
    this.messages.addAll(messages);
  }

  @Override
  public void pushMessages(List<V> messages) {

  }

  @Override
  public void close() throws IOException {

  }

  public Iterator<Pair<K,V>> messageIterator() {
    return this.messages.iterator();
  }
}
