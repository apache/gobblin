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

import java.rmi.server.UID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * A {@link Converter} that takes an Avro {@link GenericRecord} and converts it to {@link AvroGenericRecordWritable}.
 * This class is useful for integration with the {@link gobblin.converter.serde.HiveSerDeConverter}, which expects input
 * records to be of type {@link org.apache.hadoop.io.Writable}.
 */
public class AvroRecordToAvroWritableConverter
    extends Converter<Schema, Schema, GenericRecord, AvroGenericRecordWritable> {

  private final UID uid = new UID();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

  @Override
  public Iterable<AvroGenericRecordWritable> convertRecord(Schema outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit) throws DataConversionException {
    AvroGenericRecordWritable avroWritable = new AvroGenericRecordWritable();
    avroWritable.setRecord(inputRecord);
    avroWritable.setFileSchema(outputSchema);
    avroWritable.setRecordReaderID(this.uid);
    return Lists.newArrayList(avroWritable);
  }
}
