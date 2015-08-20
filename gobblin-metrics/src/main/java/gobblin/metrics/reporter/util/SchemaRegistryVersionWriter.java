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

package gobblin.metrics.reporter.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.collect.Maps;

import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;


/**
 * Implementation of {@link gobblin.metrics.reporter.util.SchemaVersionWriter} that uses a
 * {@link gobblin.metrics.kafka.KafkaAvroSchemaRegistry} to get Schema version identifier and write it to
 * {@link java.io.DataOutputStream}.
 */
public class SchemaRegistryVersionWriter implements SchemaVersionWriter {

  private final KafkaAvroSchemaRegistry registry;
  private Map<Schema, String> registrySchemaIds;
  private final String topic;

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, String topic) {
    this.registry = registry;
    this.registrySchemaIds = Maps.newConcurrentMap();
    this.topic = topic;
  }

  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream)
      throws IOException {
    if(!this.registrySchemaIds.containsKey(schema)) {
      String schemaId = this.registry.register(schema, this.topic);
      this.registrySchemaIds.put(schema, schemaId);
    }
    outputStream.writeByte(KafkaAvroSchemaRegistry.MAGIC_BYTE);
    try {
      outputStream.write(Hex.decodeHex(this.registrySchemaIds.get(schema).toCharArray()));
    } catch(DecoderException exception) {
      throw new IOException(exception);
    }
  }

  @Override
  public Object readSchemaVersioningInformation(DataInputStream inputStream)
      throws IOException {
    if(inputStream.readByte() != KafkaAvroSchemaRegistry.MAGIC_BYTE) {
      throw new IOException("MAGIC_BYTE not found in Avro message.");
    }
    throw new UnsupportedOperationException("readSchemaVersioningInformation not implemented for schema registry.");
  }
}
