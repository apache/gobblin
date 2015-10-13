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

package gobblin.writer.test;

import gobblin.configuration.State;
import gobblin.writer.WriterPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


public class TestPartitioner implements WriterPartitioner<String> {

  public static final String PARTITION = "partition";

  private static final Schema SCHEMA = SchemaBuilder.record("ArticleTitle").fields().name(PARTITION).
      type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();

  public TestPartitioner(State state) {
  }

  @Override public Schema partitionSchema() {
    return SCHEMA;
  }

  @Override public GenericRecord partitionForRecord(String record) {
    GenericRecord partition = new GenericData.Record(SCHEMA);
    partition.put(PARTITION, record.toLowerCase().charAt(0));
    return partition;
  }
}
