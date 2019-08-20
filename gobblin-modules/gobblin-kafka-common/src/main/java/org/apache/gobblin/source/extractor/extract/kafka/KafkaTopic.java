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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.State;


/**
 * A kafka topic is composed of a topic name, and a list of partitions.
 *
 * @author Ziyang Liu
 *
 */
public final class KafkaTopic {
  private final String name;
  private final List<KafkaPartition> partitions;
  private Optional<State> topicSpecificState;

  public KafkaTopic(String name, List<KafkaPartition> partitions) {
    this(name, partitions, Optional.absent());
  }

  public KafkaTopic(String name, List<KafkaPartition> partitions, Optional<State> topicSpecificState) {
    this.name = name;
    this.partitions = Lists.newArrayList();
    for (KafkaPartition partition : partitions) {
      this.partitions.add(new KafkaPartition(partition));
    }
    this.topicSpecificState = topicSpecificState;
  }

  public String getName() {
    return this.name;
  }

  public List<KafkaPartition> getPartitions() {
    return Collections.unmodifiableList(this.partitions);
  }

  public Optional<State> getTopicSpecificState() {
    return this.topicSpecificState;
  }
}
