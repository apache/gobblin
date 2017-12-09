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

package org.apache.gobblin.writer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.writer.partitioner.WriterPartitioner;


/**
 * A {@link DataWriterBuilder} used with a {@link WriterPartitioner}. When provided with a partitioner, Gobblin will create a
 * {@link org.apache.gobblin.writer.DataWriter} per partition. All partitions will be build with identical builders, except
 * that {@link #forPartition} will specify the partition.
 *
 * <p>
 *   The contract with the {@link PartitionAwareDataWriterBuilder} is as follows:
 *   * Gobblin will call {@link #validatePartitionSchema(Schema)} before calling build().
 *   * Gobblin is guaranteed to call {@link #validatePartitionSchema(Schema)} for some instance of
 *           {@link PartitionAwareDataWriterBuilder} with the same class, but not necessarily for the specific instance
 *           that will be used to build the {@link DataWriter}.
 *   * If !partition1.equals(partition2), then Gobblin may call build a writer for partition1 and a writer for
 *           partition2 in the same job. This should not cause an exception.
 *   * If partition1.equals(partition2), a single fork will not build writers for both partitions.
 * </p>
 *
 * <p>
 *   The summary is:
 *   * Make sure {@link #validatePartitionSchema} returns false if the writer can't handle the schema.
 *   * {@link #validatePartitionSchema} should not have any side effects on the {@link PartitionAwareDataWriterBuilder}.
 *   * Different partitions should generate non-colliding writers.
 * </p>
 */
@Slf4j
public abstract class PartitionAwareDataWriterBuilder<S, D> extends DataWriterBuilder<S, D> {

  protected Optional<GenericRecord> partition = Optional.absent();

  /**
   * Sets the partition that the build {@link DataWriter} will handle.
   * @param partition A {@link GenericRecord} specifying the partition.
   * @return A {@link PartitionAwareDataWriterBuilder}.
   */
  public PartitionAwareDataWriterBuilder<S, D> forPartition(GenericRecord partition) {
    this.partition = Optional.fromNullable(partition);
    log.debug("For partition {}", this.partition);
    return this;
  }

  /**
   * Checks whether the {@link PartitionAwareDataWriterBuilder} is compatible with a given partition {@link Schema}.
   * If this method returns false, the execution will crash with an error. If this method returns true, the
   * {@link DataWriterBuilder} is expected to be able to understand the partitioning schema and handle it correctly.
   * @param partitionSchema {@link Schema} of {@link GenericRecord} objects that will be passed to {@link #forPartition}.
   * @return true if the {@link DataWriterBuilder} can understand the schema and is able to generate partitions from
   *        this schema.
   */
  public abstract boolean validatePartitionSchema(Schema partitionSchema);
}
