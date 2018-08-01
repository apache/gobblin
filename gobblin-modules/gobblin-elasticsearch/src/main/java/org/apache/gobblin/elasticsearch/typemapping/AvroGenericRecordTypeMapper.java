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

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import com.google.common.io.Closer;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;


/**
 * A TypeMapper for Avro GenericRecords.
 */
@Slf4j
public class AvroGenericRecordTypeMapper implements TypeMapper<GenericRecord> {

  private final JsonSerializer<GenericRecord> serializer;
  private final Closer closer;

  public AvroGenericRecordTypeMapper() {
    this.closer =Closer.create();
    this.serializer = this.closer.register(new AvroGenericRecordSerializer());
  }

  @Override
  public void configure(Config config) {
    this.serializer.configure(config);
    log.info("AvroGenericRecordTypeMapper successfully configured");
  }

  @Override
  public JsonSerializer<GenericRecord> getSerializer() {
    return this.serializer;
  }

  @Override
  public String getValue(String fieldName, GenericRecord record)
      throws FieldMappingException {
    try {
      Object idValue = record.get(fieldName);
      return idValue.toString();
    }
    catch (Exception e) {
      throw new FieldMappingException("Could not find field " + fieldName, e);
    }
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
