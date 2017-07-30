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
package gobblin.converter.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * Convert an Avro GenericRecord back to its byte representation. Note: This converter returns
 * the raw bytes for a record - it does not return a container file. If you want to write
 * Avro records out to a container file do not use this converter; instead use the AvroDataWriter
 * writer.
 */
public class AvroToBytesConverter extends Converter<Schema, String, GenericRecord, byte[]> {
  private GenericDatumWriter<GenericRecord> writer;
  private ThreadLocal<BinaryEncoder> encoderCache = new ThreadLocal<BinaryEncoder>() {
    @Override
    protected BinaryEncoder initialValue() {
      return null;
    }
  };

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    writer = new GenericDatumWriter<GenericRecord>(inputSchema);
    return inputSchema.toString();
  }

  @Override
  public Iterable<byte[]> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();

      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bytesOut, encoderCache.get());
      encoderCache.set(encoder);
      writer.write(inputRecord, encoder);
      encoder.flush();

      return Collections.singleton(bytesOut.toByteArray());
    } catch (IOException e) {
      throw new DataConversionException("Error serializing record", e);
    }
  }
}
