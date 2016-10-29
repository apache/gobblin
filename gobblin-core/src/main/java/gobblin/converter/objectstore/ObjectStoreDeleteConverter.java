/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.converter.objectstore;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.DataConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;
import gobblin.writer.objectstore.ObjectStoreDeleteOperation;
import gobblin.writer.objectstore.ObjectStoreOperationBuilder;


/**
 * A converter to build {@link ObjectStoreDeleteOperation}s using an Avro {@link GenericRecord}. The object id field in
 * input avro record can be set using {@link #OBJECT_ID_FIELD}. The field name is a required property.
 *
 */
public class ObjectStoreDeleteConverter extends ObjectStoreConverter<Schema, GenericRecord, ObjectStoreDeleteOperation> {

  @VisibleForTesting
  public static final String OBJECT_ID_FIELD = "gobblin.converter.objectstore.delete.objectIdField";

  private String objectIdField;

  public ObjectStoreDeleteConverter init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(OBJECT_ID_FIELD),
        String.format("%s is a required property. ", OBJECT_ID_FIELD));
    this.objectIdField = workUnit.getProp(OBJECT_ID_FIELD);
    return this;
  }

  @Override
  public Iterable<ObjectStoreDeleteOperation> convertRecord(Class<?> outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit) throws DataConversionException {
    return new SingleRecordIterable<ObjectStoreDeleteOperation>(ObjectStoreOperationBuilder.deleteBuilder()
        .withObjectId(((byte[]) AvroUtils.getFieldValue(inputRecord, this.objectIdField).orNull())).build());
  }
}
