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

package gobblin.converter;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.filter.AvroProjectionConverter;
import gobblin.converter.filter.AvroSchemaFieldRemover;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.KafkaSchemaRegistryFactory;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.util.AvroUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import javax.xml.bind.DatatypeConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/**
 * A converter for extracting schema/records from an envelope schema.
 * Input schema: envelope schema - must have fields payloadSchemaId (the schema registry key of the output
 *               schema) and payload (byte data for output record)
 * Input record: record corresponding to input schema
 * Output schema: schema obtained from schema registry using key provided in input record's {@link #PAYLOAD_SCHEMA_ID_FIELD}
 * Output record: record corresponding to output schema obtained from input record's {@link #PAYLOAD_FIELD} as bytes
 */
public class EnvelopeSchemaConverter extends Converter<Schema, String, GenericRecord, GenericRecord> {

  public static final String PAYLOAD_SCHEMA_ID_FIELD = "EnvelopeSchemaConverter.schemaIdField";
  public static final String PAYLOAD_FIELD = "EnvelopeSchemaConverter.payloadField";
  public static final String DEFAULT_PAYLOAD_SCHEMA_ID_FIELD ="payloadSchemaId";
  public static final String DEFAULT_PAYLOAD_FIELD = "payload";
  public static final String DEFAULT_KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS = "gobblin.metrics.kafka.KafkaAvroSchemaRegistryFactory";

  private Optional<AvroSchemaFieldRemover> fieldRemover;
  private KafkaSchemaRegistry registry;
  private DecoderFactory decoderFactory;
  private LoadingCache<Schema, GenericDatumReader<GenericRecord>> readers;

  /**
   * To remove certain fields from the Avro schema or records of a topic/table, set property
   * {topic/table name}.remove.fields={comma-separated, fully qualified field names} in workUnit.
   */
  @Override
  public EnvelopeSchemaConverter init(WorkUnitState workUnit) {
    if (workUnit.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
      String removeFieldsPropName = workUnit.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY) + AvroProjectionConverter.REMOVE_FIELDS;
      if (workUnit.contains(removeFieldsPropName)) {
        this.fieldRemover = Optional.of(new AvroSchemaFieldRemover(workUnit.getProp(removeFieldsPropName)));
      } else {
        this.fieldRemover = Optional.absent();
      }
    }
    String registryFactoryField = workUnit.contains(KafkaSchemaRegistryFactory.KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS) ?
        workUnit.getProp(KafkaSchemaRegistryFactory.KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS) : DEFAULT_KAFKA_SCHEMA_REGISTRY_FACTORY_CLASS;
    try {
      KafkaSchemaRegistryFactory registryFactory = ((Class<? extends KafkaSchemaRegistryFactory>) Class.forName(registryFactoryField)).newInstance();
      this.registry = registryFactory.create(workUnit.getProperties());
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      return null;
    }
    this.decoderFactory = DecoderFactory.get();
    this.readers = CacheBuilder.newBuilder().build(new CacheLoader<Schema, GenericDatumReader<GenericRecord>>() {
      @Override
      public GenericDatumReader<GenericRecord> load(final Schema key) throws Exception {
        return new GenericDatumReader<>(key);
      }
    });
    return this;
  }

  /**
   * Do nothing, actual schema must be obtained from records.
   */
  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return EnvelopeSchemaConverter.class.getName();
  }

  /**
   * Get actual schema from registry and deserialize payload using it.
   */
  @Override
  public Iterable<GenericRecord> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      String schemaIdField = workUnit.contains(PAYLOAD_SCHEMA_ID_FIELD) ?
          workUnit.getProp(PAYLOAD_SCHEMA_ID_FIELD) : DEFAULT_PAYLOAD_SCHEMA_ID_FIELD;
      String payloadField = workUnit.contains(PAYLOAD_FIELD) ?
          workUnit.getProp(PAYLOAD_FIELD) : DEFAULT_PAYLOAD_FIELD;
      String schemaKey = String.valueOf(inputRecord.get(schemaIdField));
      Schema payloadSchema = (Schema) this.registry.getSchemaByKey(schemaKey);
      byte[] payload = getPayload(inputRecord, payloadField);
      GenericRecord outputRecord = deserializePayload(payload, payloadSchema);
      if (this.fieldRemover.isPresent()) {
        payloadSchema = this.fieldRemover.get().removeFields(payloadSchema);
      }
      return new SingleRecordIterable<>(AvroUtils.convertRecordSchema(outputRecord, payloadSchema));
    } catch (IOException | SchemaRegistryException | ExecutionException e) {
      throw new DataConversionException(e);
    }
  }

  /**
   * Get payload field from GenericRecord and convert to byte array
   */
  public byte[] getPayload(GenericRecord inputRecord, String payloadFieldName) {
    ByteBuffer bb = (ByteBuffer) inputRecord.get(payloadFieldName);
    byte[] payloadBytes;
    if (bb.hasArray()) {
      payloadBytes = bb.array();
    } else {
      payloadBytes = new byte[bb.remaining()];
      bb.get(payloadBytes);
    }
    String hexString = new String(payloadBytes, StandardCharsets.UTF_8);
    return DatatypeConverter.parseHexBinary(hexString);
  }

  /**
   * Deserialize payload using payload schema
   */
  public GenericRecord deserializePayload(byte[] payload, Schema payloadSchema) throws IOException, ExecutionException {
    Decoder decoder = this.decoderFactory.binaryDecoder(payload, null);
    GenericDatumReader<GenericRecord> reader = this.readers.get(payloadSchema);
    return reader.read(null, decoder);
  }
}
