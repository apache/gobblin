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

import java.util.Collection;

import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;


/**
 * A listener that is called when kafka partitions are re-assigned when a consumer leaves/joins a group
 *
 * For more details, See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html
 */

public interface GobblinConsumerRebalanceListener {

  void onPartitionsRevoked(Collection<KafkaPartition> partitions);

  void onPartitionsAssigned(Collection<KafkaPartition> partitions);

}
