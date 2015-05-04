/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

/**
 * A KafkaEvent contains key, value, offset and nextOffset. In the old Kafka API, they are contained
 * in the MessageAndOffset class. In the new Kafka API, they are contained in the ConsumerRecord<K, V> class.
 *
 * @param <K> Type of key. When using the Kafka Old API, K should be ByteBuffer.
 * @param <V> Type of value. When using the Kafka Old API, V should be ByteBuffer.
 *
 * @author ziliu
 */
public interface KafkaEvent<K, V> {

  /**
   * Retrieve the key of the event.
   */
  public K key();

  /**
   * Retrieve the value of the event.
   */
  public V value();

  /**
   * Retrieve the offset of the event.
   */
  public long offset();

  /**
   * Retrieve the next offset of the event.
   */
  public long nextOffset();
}
