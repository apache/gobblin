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
package org.apache.gobblin.kafka.client;

/**
 * A kafka record that provides getters for deserialized key and value. This record type can be used to wrap kafka records
 * consumed through new kafka-client consumer APIs (0.9 and above) which support serializers and deserializers.
 *
 * @param <K> Message key type. If this record does not have a key use - <b><code>?</code></b>
 * @param <V> Message value type
 */
public interface DecodeableKafkaRecord<K, V> extends KafkaConsumerRecord {
  /**
   * The key of this record. Can be null if only value exists
   */
  public K getKey();

  /**
   * The value of this record.
   */
  public V getValue();
}
