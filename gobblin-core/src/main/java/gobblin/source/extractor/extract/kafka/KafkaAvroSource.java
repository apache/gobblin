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

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.SchemaNotFoundException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;


/**
 * A {@link gobblin.source.Source} class for Kafka where events are in Avro format.
 *
 * @author ziliu
 */
public class KafkaAvroSource extends KafkaSource<Schema, GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSource.class);

  private Optional<KafkaAvroSchemaRegistry> schemaRegistry = Optional.absent();

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaAvroExtractor(state);
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    if (!this.schemaRegistry.isPresent()) {
      this.schemaRegistry = Optional.of(new KafkaAvroSchemaRegistry(state.getProperties()));
    }
    return super.getWorkunits(state);
  }

  /**
   * A {@link KafkaTopic} is qualified if its schema exists in the schema registry.
   */
  @Override
  protected boolean isTopicQualified(KafkaTopic topic) {
    Preconditions.checkState(this.schemaRegistry.isPresent(),
        "Schema registry not found. Unable to verify topic schema");

    try {
      this.schemaRegistry.get().getLatestSchemaByTopic(topic.getName());
      return true;
    } catch (SchemaNotFoundException e) {
      LOG.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", topic.getName()));
      return false;
    }
  }
}
