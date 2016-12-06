/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.converter;

import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.iterators.SingletonIterator;

import com.couchbase.client.core.lang.Tuple;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.couchbase.common.TupleDocument;


public class AvroToCouchbaseTupleConverter extends Converter<Schema, String, GenericRecord, TupleDocument> {

  private String keyField = "key";
  private String dataRecordField = "data";
  private String valueField = "data";
  private String flagsField = "flags";

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    //TODO: Use the schema and config to determine which fields to pull out
    return "";
  }

  @Override
  public Iterable<TupleDocument> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    String key = (String) inputRecord.get(keyField);
    GenericRecord data = (GenericRecord) inputRecord.get(dataRecordField);

    byte[] dataBytes = (byte[]) data.get(valueField);
    Integer flags = (Integer) data.get(flagsField);

    return new SingleRecordIterable<>(new TupleDocument(key, Tuple.create(Unpooled.wrappedBuffer(dataBytes), flags)));
  }
}
