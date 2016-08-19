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

package gobblin.kafka.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.State;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.PartitionAwareDataWriterBuilder;

/**
 * Builder that hands back a {@link KafkaDataWriter}
 */
public class KafkaDataWriterBuilder extends PartitionAwareDataWriterBuilder<Schema, GenericRecord> {
  /**
   * Build a {@link DataWriter}.
   *
   * @throws IOException if there is anything wrong building the writer
   * @return the built {@link DataWriter}
   */
  @Override
  public DataWriter<GenericRecord> build()
      throws IOException {
    State state = this.destination.getProperties();
    Properties taskProps = state.getProperties();
    return new KafkaDataWriter<>(taskProps);
  }

  /**
   * Checks whether the {@link PartitionAwareDataWriterBuilder} is compatible with a given partition {@link Schema}.
   * If this method returns false, the execution will crash with an error. If this method returns true, the
   * {@link DataWriterBuilder} is expected to be able to understand the partitioning schema and handle it correctly.
   * @param partitionSchema {@link Schema} of {@link GenericRecord} objects that will be passed to {@link #forPartition}.
   * @return true if the {@link DataWriterBuilder} can understand the schema and is able to generate partitions from
   *        this schema.
   */
  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return true;
  }
}
