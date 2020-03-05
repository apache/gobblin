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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A base {@link KafkaConsumerRecord} that with offset and valueSizeInBytes
 */
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public abstract class BaseKafkaConsumerRecord implements KafkaConsumerRecord {

  private final long offset;
  private final long valueSizeInBytes;
  private final String topic;
  private final int partitionId;
  public static final long VALUE_SIZE_UNAVAILABLE = -1l;

  @Override
  public long getOffset() {
    return this.offset;
  }

  @Override
  public long getNextOffset() {
    return this.offset + 1l;
  }

  @Override
  public long getValueSizeInBytes() {
    return this.valueSizeInBytes;
  }

  @Override
  public int getPartition() {
    return this.partitionId;
  }

  @Override
  public String getTopic() {
    return this.topic;
  }
}
