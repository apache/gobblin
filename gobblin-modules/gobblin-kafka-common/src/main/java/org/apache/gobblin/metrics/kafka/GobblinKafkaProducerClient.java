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

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;


/**
 * A simplified, generic wrapper client to communicate with Kafka. This class is (AND MUST never) depend on classes
 * defined in kafka-clients library. The request and response objects are defined in gobblin. This allows gobblin
 * sources and extractors to work with different versions of kafka. Concrete classes implementing this interface use a
 * specific version of kafka-client library.
 *
 * <p>
 * This simplified client interface supports peoducer operations required by gobblin to push to kafka.
 * </p>
 */
public interface GobblinKafkaProducerClient<K, V> extends Closeable {
  Future<WriteResponse> sendMessage(String topic, V value, Function<V, K> mapFunction, WriteCallback callback);

  /**
   * A factory to create {@link GobblinKafkaProducerClient}s
   */
  public interface GobblinKafkaProducerClientFactory {
    /**
     * Creates a new {@link GobblinKafkaProducerClient} for <code>config</code>
     *
     */
    public GobblinKafkaProducerClient create(Properties props);
  }
}
