/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.serde;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.instrumented.converter.InstrumentedConverter;
import gobblin.serde.HiveSerDeWrapper;
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * An {@link InstrumentedConverter} that takes a {@link Writable} record, uses a Hive {@link SerDe} to
 * deserialize it, and uses another Hive {@link SerDe} to serialize it into a {@link Writable} record.
 *
 * The serializer and deserializer are specified using {@link HiveSerDeWrapper#SERDE_SERIALIZER_TYPE}
 * and {@link HiveSerDeWrapper#SERDE_DESERIALIZER_TYPE}.
 *
 * @author ziliu
 */
@SuppressWarnings("deprecation")
@Slf4j
public class HiveSerDeConverter extends InstrumentedConverter<Object, Object, Writable, Writable> {

  private SerDe serializer;
  private SerDe deserializer;

  public HiveSerDeConverter init(WorkUnitState state) {
    super.init(state);
    Configuration conf = HadoopUtils.getConfFromState(state);

    try {
      this.serializer = HiveSerDeWrapper.getSerializer(state).getSerDe();
      this.deserializer = HiveSerDeWrapper.getDeserializer(state).getSerDe();
      serializer.initialize(conf, state.getProperties());
      deserializer.initialize(conf, state.getProperties());
    } catch (IOException e) {
      log.error("Failed to instantiate serializer and deserializer", e);
      throw Throwables.propagate(e);
    } catch (SerDeException e) {
      log.error("Failed to initialize serializer and deserializer", e);
      throw Throwables.propagate(e);
    }

    return this;
  }

  @Override
  public Iterable<Writable> convertRecordImpl(Object outputSchema, Writable inputRecord, WorkUnitState workUnit)
      throws DataConversionException {

    try {
      Object deserialized = this.deserializer.deserialize(inputRecord);
      Writable convertedRecord = this.serializer.serialize(deserialized, this.deserializer.getObjectInspector());
      return new SingleRecordIterable<Writable>(convertedRecord);
    } catch (SerDeException e) {
      throw new DataConversionException(e);
    }
  }

  @Override
  public Object convertSchema(Object inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return inputSchema;
  }

}
