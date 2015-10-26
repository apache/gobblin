/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.instrumented.writer;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.ImmutableList;

import gobblin.configuration.State;
import gobblin.metrics.Tag;
import gobblin.writer.DataWriter;


/**
 * {@link InstrumentedDataWriterDecorator} which add partition tags to the metric context.
 */
public class InstrumentedPartitionedDataWriterDecorator<D> extends InstrumentedDataWriterDecorator<D> {

  public static final String PARTITION = "Partition";
  private final GenericRecord partition;

  public InstrumentedPartitionedDataWriterDecorator(DataWriter<D> writer, State state, GenericRecord partition) {
    super(writer, state);
    this.partition = partition;
  }

  @Override public List<Tag<?>> generateTags(State state) {
    ImmutableList.Builder<Tag<?>> tags = ImmutableList.<Tag<?>>builder().addAll(super.generateTags(state));
    tags.add(new Tag<GenericRecord>(PARTITION, this.partition));
    for(Schema.Field field : this.partition.getSchema().getFields()) {
      tags.add(new Tag<Object>(field.name(), this.partition.get(field.name())));
    }
    return tags.build();
  }
}
