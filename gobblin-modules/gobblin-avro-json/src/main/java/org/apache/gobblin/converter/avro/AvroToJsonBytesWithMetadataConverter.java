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
package org.apache.gobblin.converter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.MetadataConverterWrapper;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.metadata.types.Metadata;


/**
 * Converts an Avro GenericRecord to a UTF-8 JSON encoded byte[]. Inserts the original recordname as content-type.
 */
public class AvroToJsonBytesWithMetadataConverter extends MetadataConverterWrapper<Schema, String, GenericRecord, byte[]> {
  private String contentType = null;

  public AvroToJsonBytesWithMetadataConverter() {
    super(new AvroToJsonBytesConverter());
  }

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    String schema = super.convertSchema(inputSchema, workUnit);
    contentType = inputSchema.getFullName() + "+json";

    return schema;
  }

  @Override
  protected Metadata convertMetadata(Metadata metadata) {
    Metadata md = super.convertMetadata(metadata);
    if (md.getGlobalMetadata().getContentType() == null) {
      md.getGlobalMetadata().setContentType(contentType);
    }

    return md;
  }
}
