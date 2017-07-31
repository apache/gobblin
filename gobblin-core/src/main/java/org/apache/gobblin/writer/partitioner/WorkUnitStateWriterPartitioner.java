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
package org.apache.gobblin.writer.partitioner;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


/**
 * A #{@link TimeBasedWriterPartitioner} that partitions an incoming set of records based purely
 * on WorkUnitState.
 */
public class WorkUnitStateWriterPartitioner extends TimeBasedWriterPartitioner<Object> {
  private final long timestamp;

  public WorkUnitStateWriterPartitioner(State state, int numBranches, int branches) {
    super(state, numBranches, branches);
    this.timestamp = calculateTimestamp(state);
  }

  @Override
  public long getRecordTimestamp(Object record) {
    return timestamp;
  }

  private long calculateTimestamp(State state) {
    long timestamp = state.getPropAsLong(ConfigurationKeys.WORK_UNIT_DATE_PARTITION_KEY, -1L);
    if (timestamp == -1L) {
      throw new IllegalArgumentException(
          "WORK_UNIT_DATE_PARTITION_KEY not present in WorkUnitState; is source an instance of DatePartitionedAvroFileSource?");
    }

    return timestamp;
  }
}
