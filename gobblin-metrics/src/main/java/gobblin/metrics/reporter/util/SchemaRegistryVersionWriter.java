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

package gobblin.metrics.reporter.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;


/**
 * Implementation of {@link gobblin.metrics.reporter.util.SchemaVersionWriter} that uses a
 * {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry} to get Schema version identifier and write it to
 * {@link java.io.DataOutputStream}.
 */
public class SchemaRegistryVersionWriter implements SchemaVersionWriter {

  private final KafkaAvroSchemaRegistry registry;
  private Map<Schema, String> registrySchemaIds;
  private final String topic;
  private final Optional<Schema> schema;
  private final Optional<String> schemaId;

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, String topic, Optional<Schema> singleSchema)
      throws IOException{
    this.registry = registry;
    this.registrySchemaIds = Maps.newConcurrentMap();
    this.topic = topic;
    this.schema = singleSchema;
    if (this.schema.isPresent()) {
      try {
        this.schemaId = Optional.of(this.registry.register(this.schema.get(), this.topic));
      } catch (SchemaRegistryException e) {
        throw Throwables.propagate(e);
      }
    } else {
      this.schemaId = Optional.absent();
    }
  }

  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream) throws IOException {

    String schemaId = this.schemaId.isPresent() ? this.schemaId.get() : this.getIdForSchema(schema);

    outputStream.writeByte(KafkaAvroSchemaRegistry.MAGIC_BYTE);
    try {
      outputStream.write(Hex.decodeHex(schemaId.toCharArray()));
    } catch (DecoderException exception) {
      throw new IOException(exception);
    }
  }

  private String getIdForSchema(Schema schema) {
    if (!this.registrySchemaIds.containsKey(schema)) {
      try {
        String schemaId = this.registry.register(schema, this.topic);
        this.registrySchemaIds.put(schema, schemaId);
      } catch (SchemaRegistryException e) {
        throw Throwables.propagate(e);
      }
    }
    return this.registrySchemaIds.get(schema);
  }

  @Override
  public Object readSchemaVersioningInformation(DataInputStream inputStream) throws IOException {
    if (inputStream.readByte() != KafkaAvroSchemaRegistry.MAGIC_BYTE) {
      throw new IOException("MAGIC_BYTE not found in Avro message.");
    }
    throw new UnsupportedOperationException("readSchemaVersioningInformation not implemented for schema registry.");
  }
}
