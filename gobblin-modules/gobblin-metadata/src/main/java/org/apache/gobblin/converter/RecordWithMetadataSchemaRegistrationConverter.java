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
package org.apache.gobblin.converter;

import java.util.Collections;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metadata.types.Metadata;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistryFactory;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.type.RecordWithMetadata;

import com.google.common.base.Preconditions;


/**
 * A converter that takes {@link RecordWithMetadata},
 * tries to register the Avro Schema with KafkaSchemaRegistry
 * and returns a {@link RecordWithMetadata} with schemaId inside Metadata
 */
public class RecordWithMetadataSchemaRegistrationConverter extends Converter<String, String, RecordWithMetadata<?>, RecordWithMetadata<?>> {
  private static final String SCHEMA_ID_KEY = "Schema-Id";
  private static final String CONTENT_TYPE = "application/avro";
  private String schemaId;

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    Schema schema = Schema.parse(inputSchema);
    if (null == schemaId) {
      try {
        schemaId = getSchemaId(workUnit.getProperties(), schema);
      } catch (SchemaRegistryException e) {
        throw new SchemaConversionException(e);
      }
    }
    return schema.toString();
  }

  private static String getSchemaId(Properties properties, Schema schema)
      throws SchemaRegistryException {
    KafkaAvroSchemaRegistry kafkaAvroSchemaRegistry =
        (KafkaAvroSchemaRegistry) new KafkaAvroSchemaRegistryFactory().create(properties);
    return kafkaAvroSchemaRegistry.register(schema);
  }

  @Override
  public Iterable<RecordWithMetadata<?>> convertRecord(String outputSchema, RecordWithMetadata<?> inputRecord,
      WorkUnitState workUnit)
      throws DataConversionException {
    Preconditions.checkNotNull(schemaId);
    Metadata metadata = inputRecord.getMetadata();
    metadata.getGlobalMetadata().setContentType(CONTENT_TYPE);
    metadata.getRecordMetadata().put(SCHEMA_ID_KEY, schemaId);
    return Collections.singleton(new RecordWithMetadata<>(inputRecord.getRecord(), metadata));
  }
}
