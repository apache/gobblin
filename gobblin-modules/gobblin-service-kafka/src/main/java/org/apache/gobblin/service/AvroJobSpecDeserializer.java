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
package org.apache.gobblin.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import org.apache.gobblin.metrics.reporter.util.FixedSchemaVersionWriter;
import org.apache.gobblin.metrics.reporter.util.SchemaVersionWriter;
import org.apache.gobblin.runtime.job_spec.AvroJobSpec;

import lombok.extern.slf4j.Slf4j;

@Slf4j
/**
 * A deserializer that converts a byte array into an {@link AvroJobSpec}
 */
public class AvroJobSpecDeserializer implements Deserializer<AvroJobSpec> {
  private BinaryDecoder _decoder;
  private SpecificDatumReader<AvroJobSpec> _reader;
  private SchemaVersionWriter<?> _versionWriter;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    InputStream dummyInputStream = new ByteArrayInputStream(new byte[0]);
    _decoder = DecoderFactory.get().binaryDecoder(dummyInputStream, null);
    _reader = new SpecificDatumReader<AvroJobSpec>(AvroJobSpec.SCHEMA$);
    _versionWriter = new FixedSchemaVersionWriter();
  }

  @Override
  public AvroJobSpec deserialize(String topic, byte[] data) {
    try (InputStream is = new ByteArrayInputStream(data)) {
      _versionWriter.readSchemaVersioningInformation(new DataInputStream(is));

      Decoder decoder = DecoderFactory.get().binaryDecoder(is, _decoder);

      return _reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Could not decode message");
    }
  }

  @Override
  public void close() {
  }
}