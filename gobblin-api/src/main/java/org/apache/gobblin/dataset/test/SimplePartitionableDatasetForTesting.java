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

package org.apache.gobblin.dataset.test;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.gobblin.dataset.PartitionableDataset;

import lombok.EqualsAndHashCode;


/**
 * A {@link PartitionableDataset} that just returns a predefined set of {@link SimpleDatasetPartitionForTesting} used for testing.
 */
@EqualsAndHashCode
public class SimplePartitionableDatasetForTesting implements PartitionableDataset<SimpleDatasetPartitionForTesting> {
  private final String urn;
  private final List<SimpleDatasetPartitionForTesting> partitions;

  public SimplePartitionableDatasetForTesting(String urn, List<SimpleDatasetPartitionForTesting> partitions) {
    this.urn = urn;
    this.partitions = partitions;
    for (SimpleDatasetPartitionForTesting partition : this.partitions) {
      partition.setDataset(this);
    }
  }

  @Override
  public String datasetURN() {
    return this.urn;
  }

  @Override
  public Stream<SimpleDatasetPartitionForTesting> getPartitions(int desiredCharacteristics,
      Comparator<SimpleDatasetPartitionForTesting> suggestedOrder) throws IOException {
    return this.partitions.stream();
  }
}
