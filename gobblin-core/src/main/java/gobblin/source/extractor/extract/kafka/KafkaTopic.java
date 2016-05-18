/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;


/**
 * A kafka topic is composed of a topic name, and a list of partitions.
 *
 * @author Ziyang Liu
 *
 */
public final class KafkaTopic {
  private final String name;
  private final List<KafkaPartition> partitions;

  public KafkaTopic(String name, List<KafkaPartition> partitions) {
    this.name = name;
    this.partitions = Lists.newArrayList();
    for (KafkaPartition partition : partitions) {
      this.partitions.add(new KafkaPartition(partition));
    }
  }

  public String getName() {
    return this.name;
  }

  public List<KafkaPartition> getPartitions() {
    return Collections.unmodifiableList(this.partitions);
  }

}
