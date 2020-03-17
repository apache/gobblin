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

import java.util.concurrent.TimeUnit;


/**
 * A kafka message/record consumed from {@link GobblinKafkaConsumerClient}. This interface provides APIs to read message
 * metadata. Extension interfaces like {@link DecodeableKafkaRecord} or {@link ByteArrayBasedKafkaRecord} provide APIs
 * to read the actual message/record.
 */
public interface KafkaConsumerRecord {

  /**
   * Offset of this record
   */
  public long getOffset();

  /**
   * Next offset after this record
   */
  public long getNextOffset();

  /**
   * Size of the message in bytes. {@value BaseKafkaConsumerRecord#VALUE_SIZE_UNAVAILABLE} if kafka-client version
   * does not provide size (like Kafka 09 clients)
   */
  public long getValueSizeInBytes();

  /**
   * @return the timestamp of the underlying ConsumerRecord.
   */
  public default long getTimestamp() { return 0; }

  /**
   * @return true if the timestamp in the ConsumerRecord is the timestamp when the record is written to Kafka.
   */
  public default boolean isTimestampLogAppend() {
    return false;
  }

  /**
   * @return Partition id for this record
   */
  int getPartition();

  /**
   * @return topic for this record
   */
  String getTopic();

  /**
   * @param fieldName the field name containing the record creation time.
   * @param timeUnit the timeunit for the timestamp field.
   * @return the record creation timestamp, if it is available. Defaults to 0.
   */
  public default long getRecordCreationTimestamp(String fieldName, TimeUnit timeUnit) { return 0; }
}
