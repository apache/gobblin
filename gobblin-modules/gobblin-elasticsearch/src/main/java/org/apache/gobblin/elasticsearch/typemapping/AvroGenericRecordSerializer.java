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
package org.apache.gobblin.elasticsearch.typemapping;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;


/**
 * A {@link JsonSerializer} for {@link GenericRecord} objects.
 */
@Slf4j
public class AvroGenericRecordSerializer implements JsonSerializer<GenericRecord> {

  private final ByteArrayOutputStream byteArrayOutputStream;
  private final DataOutputStream out;
  private final GenericDatumWriter<GenericRecord> writer;
  private final Closer closer;


  public AvroGenericRecordSerializer() {
    this.closer =Closer.create();
    this.byteArrayOutputStream = new ByteArrayOutputStream();
    this.out = this.closer.register(new DataOutputStream(this.byteArrayOutputStream));
    this.writer = new GenericDatumWriter<GenericRecord>();
  }

  @Override
  public void configure(Config config) {

  }

  @Override
  public synchronized byte[] serializeToJson(GenericRecord serializable)
      throws SerializationException {
    try {
      /**
       * We use the toString method of Avro to flatten the JSON for optional nullable types.
       * Otherwise the JSON has an additional level of nesting to encode the type.
       * e.g. "id": {"string": "id-value"} versus "id": "id-value"
       * See {@link: https://issues.apache.org/jira/browse/AVRO-1582} for a good discussion on this.
       */
      String serialized = serializable.toString();
      return serialized.getBytes(Charset.forName("UTF-8"));

    } catch (Exception exception) {
      throw new SerializationException("Could not serializeToJson Avro record", exception);
    }
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
