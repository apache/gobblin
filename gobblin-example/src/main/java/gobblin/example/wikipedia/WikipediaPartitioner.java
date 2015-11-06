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

package gobblin.example.wikipedia;

import gobblin.configuration.State;
import gobblin.writer.WriterPartitioner;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;


/**
 * Partitioner that splits records by title.
 */
public class WikipediaPartitioner implements WriterPartitioner<GenericRecord> {

  private static final String TITLE = "title";

  private static final Schema SCHEMA = SchemaBuilder.record("ArticleTitle").namespace("gobblin.example.wikipedia").
      fields().name(TITLE).
      type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();

  public WikipediaPartitioner(State state) {
  }

  @Override public Schema partitionSchema() {
    return SCHEMA;
  }

  @Override public GenericRecord partitionForRecord(GenericRecord record) {
    GenericRecord partition = new GenericData.Record(SCHEMA);
    partition.put(TITLE, record.get("title"));
    return partition;
  }
}
