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
package gobblin.converter.objectstore;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import gobblin.annotation.Alpha;
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
 * Supports objectIdField schema types string, int, long and bytes.
 *
 */
@Alpha
public class ObjectStoreDeleteConverter extends ObjectStoreConverter<Schema, GenericRecord, ObjectStoreDeleteOperation> {

  @VisibleForTesting
  public static final String OBJECT_ID_FIELD = "gobblin.converter.objectstore.delete.objectIdField";

  private String objectIdField;

  @Override
  public ObjectStoreDeleteConverter init(WorkUnitState workUnit) {
    Preconditions.checkArgument(workUnit.contains(OBJECT_ID_FIELD),
        String.format("%s is a required property. ", OBJECT_ID_FIELD));
    this.objectIdField = workUnit.getProp(OBJECT_ID_FIELD);
    return this;
  }

  @Override
  public Iterable<ObjectStoreDeleteOperation> convertRecord(Class<?> outputSchema, GenericRecord inputRecord,
      WorkUnitState workUnit) throws DataConversionException {
    Optional<Object> fieldValue = AvroUtils.getFieldValue(inputRecord, this.objectIdField);
    byte[] objectId;
    if (fieldValue.isPresent()) {
      if (fieldValue.get() instanceof Utf8) {
        objectId = ((Utf8) fieldValue.get()).getBytes();
      } else if (fieldValue.get() instanceof String) {
        objectId = ((String) fieldValue.get()).getBytes(Charsets.UTF_8);
      } else if (fieldValue.get() instanceof Long) {
        objectId = Longs.toByteArray((Long) fieldValue.get());
      } else if (fieldValue.get() instanceof Integer) {
        objectId = Ints.toByteArray((Integer) fieldValue.get());
      } else {
        objectId = (byte[]) fieldValue.get();
      }
      return new SingleRecordIterable<ObjectStoreDeleteOperation>(ObjectStoreOperationBuilder.deleteBuilder()
          .withObjectId(objectId).build());
    } else {
      throw new DataConversionException(String.format("Object Id field %s not found in record %s", this.objectIdField,
          inputRecord));
    }
  }
}
