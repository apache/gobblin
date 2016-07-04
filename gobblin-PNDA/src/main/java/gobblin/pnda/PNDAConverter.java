/*
 * Copyright (c) 2016 Cisco and/or its affiliates.
 *
 * This software is licensed to you under the terms of the Apache License,
 * Version 2.0 (the "License"). You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * The code, technical concepts, and all information contained herein, are the
 * property of Cisco Technology, Inc. and/or its affiliated entities, under
 * various laws including copyright, international treaties, patent, and/or
 * contract. Any use of the material herein must be in accordance with the
 * terms of the License.  All rights not expressly granted by the License are
 * reserved.
 *
 * Unless required by applicable law or agreed to separately in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.pnda;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.AvroRuntimeException;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.DatasetNotFoundException;

import gobblin.configuration.WorkUnitState;
import gobblin.configuration.ConfigurationKeys;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.EmptyIterable;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;

/**
 * An implementation of {@link Converter}.
 *
 * <p>
 *   This converter converts the input string schema into an
 *   Avro {@link org.apache.avro.Schema} and each input byte[] document
 *   into an Avro {@link org.apache.avro.generic.GenericRecord}.
 * </p>
 */
public class PNDAConverter extends Converter<String,
                                              Schema,
                                              byte[],
                                              GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(PNDAConverter.class);
  private GenericDatumReader<Record> reader;

  /* Error handling attributes */
  public static final String KITE_ERROR_DATASET_URI = "PNDA.quarantine.dataset.uri";
  private long loggedErrors;
  public static final long MAX_LOGGED_ERRORS = 10;
  private DatasetWriter errorWriter = null;
  private Schema errorSchema = null;
  private String topic;

  public PNDAConverter init(WorkUnitState workUnit) {
    this.loggedErrors = 0;
    String errorDatasetUri = workUnit.getProp(KITE_ERROR_DATASET_URI);
    if (errorDatasetUri != null) {
      try {
        Dataset quarantine = Datasets.load(errorDatasetUri);
        this.errorSchema = quarantine.getDescriptor().getSchema();
        this.errorWriter = quarantine.newWriter();
      } catch (DatasetNotFoundException error) {
        log.error(String.format("Unable to load Quarantine Dataset at %s. Bad data will be ignored",
                                errorDatasetUri));
      }
    } else {
      log.error(String.format("'%s' configuration property not set. Bad data "
                              + "will be ignored", KITE_ERROR_DATASET_URI));
    }

    log.info(String.format("Messages that are not in the PNDA format will "
                           + "be written to '%s'", errorDatasetUri));

    return this;
  }

  public void close() throws IOException {
    if (this.errorWriter != null) {
      this.errorWriter.close();
    }
  }

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    /*
     * As we are creating a PNDA specific converter, we ignore the
     * inputSchema parameter.
     * In our case, the source is a KafkaSimpleExtractor which give the topic
     * name as the inputSchema, this is not a AVRO schema.
     */

    Schema schema = null;
    String sourceSchema = workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA);

    this.topic = inputSchema; // Save topic name

    if (sourceSchema == null || sourceSchema.isEmpty()) {
      throw new IllegalArgumentException(
                  String.format("%s configuration parameter cannot be empty",
                                ConfigurationKeys.SOURCE_SCHEMA));
    }

    if (sourceSchema.startsWith("{")) {
      log.info(String.format("Using the following AVRO schema: %s", sourceSchema));
      schema = new Schema.Parser().parse(sourceSchema);
    } else {
      log.info(String.format("Using the following AVRO schema file: %s", sourceSchema));
      try {
        new Schema.Parser().parse(new File(sourceSchema));
      } catch (IOException error) {
        throw new SchemaConversionException(
                        String.format("Unable to read AVRO schema file %s", error));
      }
    }

    this.reader = new GenericDatumReader<>(schema);

    return schema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema schema, byte[] inputRecord,
                                               WorkUnitState workUnit)
      throws DataConversionException {

    Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(inputRecord, null);
    try {
      GenericRecord record = reader.read(null, binaryDecoder);
      return new SingleRecordIterable<>(record);
    } catch (IOException | AvroRuntimeException error) {
      writeErrorData(inputRecord, "Unable to deserialize data");
      return new EmptyIterable<>();
    }
  }

  private void writeErrorData(byte[] inputRecord, String reason) {
    if (this.errorWriter == null) {
      return;
    }

    /* Only log at most MAX_LOGGED_ERRORS messages */
    this.loggedErrors++;
    if (this.loggedErrors < PNDAConverter.MAX_LOGGED_ERRORS) {
      log.error(String.format("A record from topic '%s' was not deserizable,"
                              + "it was put in quarantine", this.topic));
    } else if (this.loggedErrors == PNDAConverter.MAX_LOGGED_ERRORS) {
      log.error(String.format("Stopping logging deserialization errors "
                              + "after %d messages", PNDAConverter.MAX_LOGGED_ERRORS));
    }
    GenericRecord errorRecord = new GenericData.Record(this.errorSchema);

    errorRecord.put("topic", this.topic);
    errorRecord.put("timestamp", System.currentTimeMillis());
    errorRecord.put("reason", reason);
    errorRecord.put("payload", inputRecord);

    this.errorWriter.write(errorRecord);
  }
}
