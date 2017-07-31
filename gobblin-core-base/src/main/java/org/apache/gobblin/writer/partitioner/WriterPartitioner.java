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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Partitions records in the writer phase.
 *
 * Implementations must have a constructor with signature <init>({@link org.apache.gobblin.configuration.State}).
 */
public interface WriterPartitioner<D> {

  /**
   * @return The schema that {@link GenericRecord} returned by {@link #partitionForRecord} will have.
   */
  public Schema partitionSchema();

  /**
   * Returns the partition that the input record belongs to. If
   * partitionFoRecord(record1).equals(partitionForRecord(record2)), then record1 and record2
   * belong to the same partition.
   * @param record input to compute partition for.
   * @return {@link GenericRecord} representing partition record belongs to.
   */
  public GenericRecord partitionForRecord(D record);

}
