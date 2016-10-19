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

package gobblin.writer.partitioner;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import gobblin.configuration.State;


/**
 * A {@link WriterPartitioner} that partitions a record based on its schema. Partition record is returned with
 * field {@link #SCHEMA_STRING} containing the record's schema as a string.
 */
public class SchemaBasedWriterPartitioner implements WriterPartitioner<GenericRecord> {

  public static final String SCHEMA_STRING = "schemaString";
  private static final Schema SCHEMA = SchemaBuilder.record("Schema").namespace("gobblin.writer.partitioner")
      .fields().name(SCHEMA_STRING).type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();

  public SchemaBasedWriterPartitioner(State state, int numBranches, int branchId) {}

  @Override
  public Schema partitionSchema() {
    return SCHEMA;
  }

  @Override
  public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(SCHEMA);
    partition.put(SCHEMA_STRING, record.getSchema().toString());
    return partition;
  }
}
