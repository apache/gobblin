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

package gobblin.source.extractor.extract.kafka;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * A {@link gobblin.source.Source} class for Kafka where events are in Avro format.
 *
 * @author ziliu
 */
public class KafkaAvroSource extends KafkaSource<Schema, GenericRecord> {
  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaAvroExtractor(state);
  }
}
