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

import java.io.IOException;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * Converter that can take a single binary encoded Avro record and convert it to a
 * GenericRecord object.
 */
public class BytesToAvroConverter extends Converter<String, Schema, byte[], GenericRecord> {
  private Schema latestSchema = null;
  private GenericDatumReader<GenericRecord> recordReader = null;
  private ThreadLocal<BinaryDecoder> decoderCache = new ThreadLocal<BinaryDecoder>() {
    @Override
    protected BinaryDecoder initialValue() {
      return null;
    }
  };

  @Override
  public Schema convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
      latestSchema = new Schema.Parser().parse(inputSchema);
      recordReader = new GenericDatumReader<>(latestSchema);
      return latestSchema;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Preconditions.checkNotNull(recordReader, "Must have called convertSchema!");

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputRecord, decoderCache.get());
    try {
      GenericRecord parsedRecord = recordReader.read(null, decoder);
      decoderCache.set(decoder);
      return Collections.singleton(parsedRecord);
    } catch (IOException e) {
      throw new DataConversionException("Error parsing record", e);
    }

  }
}
