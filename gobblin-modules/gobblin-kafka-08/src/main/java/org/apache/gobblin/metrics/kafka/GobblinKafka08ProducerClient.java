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

package org.apache.gobblin.metrics.kafka;

import com.google.common.io.Closer;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.gobblin.kafka.writer.Kafka08DataWriter;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.gobblin.writer.WriteResponseFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Implementation of GobblinKafkaProducerClient that wraps a {@link KafkaProducer}.
 * This provides at-least once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
public class GobblinKafka08ProducerClient<K, V> implements GobblinKafkaProducerClient<K, V> {
  KafkaProducer<K, V> producer;
  Closer closer;

  GobblinKafka08ProducerClient(Properties props) {
    this.closer = Closer.create();
    this.producer = closer.register(new KafkaProducer<K, V>(props));
  }

  @Override
  public Future<WriteResponse> sendMessage(String topic, V value, Function<V, K> mapFunction, WriteCallback callback) {
    return new WriteResponseFuture<>(
        this.producer.send(new ProducerRecord<>(topic, mapFunction.apply(value), value), (metadata, exception) -> {
          if (exception != null) {
            callback.onFailure(exception);
          } else {
            callback.onSuccess(Kafka08DataWriter.WRITE_RESPONSE_WRAPPER.wrap(metadata));
          }
        }), Kafka08DataWriter.WRITE_RESPONSE_WRAPPER);
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  public class GobblinKafka08ProducerClientFactory implements GobblinKafkaProducerClientFactory {
    /**
     * Creates a new {@link GobblinKafkaProducerClient} for <code>config</code>
     *
     */
    public GobblinKafka08ProducerClient create(Properties props) {
      return new GobblinKafka08ProducerClient(props);
    }
  }
}
