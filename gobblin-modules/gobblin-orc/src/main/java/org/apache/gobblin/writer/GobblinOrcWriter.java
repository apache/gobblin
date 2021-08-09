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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.orc.TypeDescription;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;

/**
 * A wrapper for ORC-core writer without dependency on Hive SerDe library.
 */
@Slf4j
public class GobblinOrcWriter extends GobblinBaseOrcWriter<Schema, GenericRecord> {
  public GobblinOrcWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties) throws IOException {
    super(builder, properties);
  }

  @Override
  protected TypeDescription getOrcSchema() {
    return AvroOrcSchemaConverter.getOrcSchema(this.inputSchema);
  }

  @Override
  protected OrcValueWriter<GenericRecord> getOrcValueWriter(TypeDescription typeDescription, Schema inputSchema,
      State state) {
    return new GenericRecordToOrcValueWriter(typeDescription, this.inputSchema, this.properties);
  }

  @Override
  protected Properties getPropsWithOrcSchema() throws SerDeException {
    Properties properties = new Properties();

    properties.setProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), this.inputSchema.toString());
    AvroObjectInspectorGenerator aoig = new AvroObjectInspectorGenerator(this.inputSchema);

    properties.setProperty("columns", StringUtils.join(aoig.getColumnNames(), ","));
    properties.setProperty("columns.types", StringUtils.join(aoig.getColumnTypes(), ","));

    return properties;
  }

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == GobblinOrcWriter.class;
  }
}
