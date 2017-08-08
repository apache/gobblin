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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Implementation of {@link org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter} that uses a
 * {@link org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry} to get Schema version identifier and write it to
 * {@link java.io.DataOutputStream}.
 */
public class SchemaRegistryVersionWriter implements SchemaVersionWriter<Schema> {

  private final KafkaAvroSchemaRegistry registry;
  private Map<Schema, String> registrySchemaIds;
  private final Optional<String> overrideName;
  private final Optional<Schema> schema;
  private final Optional<String> schemaId;
  private final int schemaIdLengthBytes;

  public SchemaRegistryVersionWriter(Config config)
      throws IOException {
    this(new KafkaAvroSchemaRegistry(ConfigUtils.configToProperties(config)), Optional.<String>absent(),
        Optional.<Schema>absent());
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, String overrideName)
      throws IOException {
    this(registry, overrideName, Optional.<Schema>absent());
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, String overrideName,
      Optional<Schema> singleSchema)
      throws IOException {
    this(registry, Optional.of(overrideName), singleSchema);
  }

  public SchemaRegistryVersionWriter(KafkaAvroSchemaRegistry registry, Optional<String> overrideName,
      Optional<Schema> singleSchema)
      throws IOException {
    this.registry = registry;
    this.registrySchemaIds = Maps.newConcurrentMap();
    this.overrideName = overrideName;
    this.schema = singleSchema;
    this.schemaIdLengthBytes = registry.getSchemaIdLengthByte();
    if (this.schema.isPresent()) {
      try {
        this.schemaId = this.overrideName.isPresent() ? Optional
            .of(this.registry.register(this.schema.get(), this.overrideName.get()))
            : Optional.of(this.registry.register(this.schema.get()));
      } catch (SchemaRegistryException e) {
        throw Throwables.propagate(e);
      }
    } else {
      this.schemaId = Optional.absent();
    }
  }

  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream)
      throws IOException {

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
        String schemaId = this.overrideName.isPresent() ? this.registry.register(schema, this.overrideName.get())
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
    String hexKey = Hex.encodeHexString(byteKey);

    try {
      return this.registry.getSchemaByKey(hexKey);
    } catch (SchemaRegistryException sre) {
      throw new IOException("Failed to retrieve schema for key " + hexKey, sre);
    }
  }
}
