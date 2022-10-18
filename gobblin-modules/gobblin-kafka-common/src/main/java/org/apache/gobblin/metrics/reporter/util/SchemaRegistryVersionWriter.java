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

package org.apache.gobblin.metrics.reporter.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Implementation of {@link org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter} that uses a
 * {@link org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry} to get Schema version identifier and write it to
 * {@link java.io.DataOutputStream}.
 */
@Slf4j
public class SchemaRegistryVersionWriter implements SchemaVersionWriter<Schema> {

  private final KafkaAvroSchemaRegistry registry;
  private Map<Schema, String> registrySchemaIds;
  private final String overrideName;
  private final Schema schema;
  private final String schemaId;
  private final int schemaIdLengthBytes;

  public SchemaRegistryVersionWriter(Config config)
      throws SchemaRegistryException {
    this(new KafkaAvroSchemaRegistry(ConfigUtils.configToProperties(config)), null, null, null);
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, @Nullable String overrideName) throws SchemaRegistryException {
    this(registry, overrideName, null);
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, @Nullable String overrideName, @Nullable Schema singleSchema)
      throws SchemaRegistryException {
    this(registry, overrideName, singleSchema, null);
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, @Nullable String overrideName, @Nullable Schema singleSchema, @Nullable String schemaId)
      throws SchemaRegistryException {
    this.registry = registry;
    this.registrySchemaIds = Maps.newConcurrentMap();
    this.overrideName = overrideName;
    this.schema = singleSchema;
    this.schemaIdLengthBytes = registry.getSchemaIdLengthByte();
    if ((this.schema != null) && (schemaId == null)) {
      this.schemaId =
          (!Strings.isNullOrEmpty(this.overrideName)) ? this.registry.register(this.schema, this.overrideName)
              : this.registry.register(this.schema);
    } else {
      if (schemaId != null) {
        log.info("Skipping registering schema with schema registry. Using schema with id: {}", schemaId);
      }
      this.schemaId = schemaId;
    }
  }

  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream)
      throws IOException {

    String schemaId = this.schemaId != null ? this.schemaId : this.getIdForSchema(schema);

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
        String schemaId = !Strings.isNullOrEmpty(this.overrideName) ? this.registry.register(schema, this.overrideName)
            : this.registry.register(schema);
        this.registrySchemaIds.put(schema, schemaId);
      } catch (SchemaRegistryException e) {
        throw Throwables.propagate(e);
      }
    }
    return this.registrySchemaIds.get(schema);
  }

  @Override
  public Schema readSchemaVersioningInformation(DataInputStream inputStream)
      throws IOException {
    String hexKey = getSchemaHexKey(inputStream);

    try {
      return this.registry.getSchemaByKey(hexKey);
    } catch (SchemaRegistryException sre) {
      throw new IOException("Failed to retrieve schema for key " + hexKey, sre);
    }
  }

  @Override
  public void advanceInputStreamToRecord(DataInputStream inputStream) throws IOException {
    getSchemaHexKey(inputStream);
  }

  private String getSchemaHexKey(DataInputStream inputStream) throws IOException {
    if (inputStream.readByte() != KafkaAvroSchemaRegistry.MAGIC_BYTE) {
      throw new IOException("MAGIC_BYTE not found in Avro message.");
    }

    byte[] byteKey = new byte[schemaIdLengthBytes];
    int bytesRead = inputStream.read(byteKey, 0, schemaIdLengthBytes);
    if (bytesRead != schemaIdLengthBytes) {
      throw new IOException(String
          .format("Could not read enough bytes for schema id. Expected: %d, found: %d.", schemaIdLengthBytes,
              bytesRead));
    }
    return Hex.encodeHexString(byteKey);
  }
}
