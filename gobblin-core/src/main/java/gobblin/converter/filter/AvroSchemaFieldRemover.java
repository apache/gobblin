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

package gobblin.converter.filter;

import gobblin.converter.SchemaConversionException;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.lang3.mutable.MutableInt;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * A class that removes specific fields from a (possibly recursive) Avro schema.
 *
 * @author ziliu
 */
public class AvroSchemaFieldRemover {

  private static final Splitter SPLITTER_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Splitter SPLITTER_ON_DOT = Splitter.on('.').trimResults().omitEmptyStrings();

  private static final AvroSchemaFieldRemover DO_NOTHING_INSTANCE = new AvroSchemaFieldRemover();

  private final Map<String, AvroSchemaFieldRemover> children = Maps.newHashMap();
  private List<String> fieldsToRemove;

  /**
   * @param fieldNames Field names to be removed from the Avro schema. Contains comma-separated fully-qualified
   * field names, e.g., "header.memberId,mobileHeader.osVersion".
   */
  public AvroSchemaFieldRemover(String fieldNames) {
    this.fieldsToRemove = SPLITTER_ON_COMMA.splitToList(fieldNames);
    this.addChildren(fieldsToRemove);
  }

  private AvroSchemaFieldRemover() {
    this("");
  }

  private void addChildren(List<String> fields) {
    for (String fieldName : fields) {
      List<String> fieldNameComponents = SPLITTER_ON_DOT.splitToList(fieldName);
      if (!fieldNameComponents.isEmpty()) {
        this.addChildren(fieldNameComponents, 0);
      }
    }
  }

  private void addChildren(List<String> fieldNameComponents, int level) {
    Preconditions.checkArgument(fieldNameComponents.size() > level);

    if (!this.children.containsKey(fieldNameComponents.get(level))) {
      this.children.put(fieldNameComponents.get(level), new AvroSchemaFieldRemover());
    }
    if (level < fieldNameComponents.size() - 1) {
      this.children.get(fieldNameComponents.get(level)).addChildren(fieldNameComponents, level + 1);
    }
  }

  /**
   * @param schema The Avro schema where the specified fields should be removed from.
   * @return A new Avro schema with the specified fields removed.
   */
  public Schema removeFields(Schema schema) {
    return removeFields(schema, Maps.<String, Schema> newHashMap(), new MutableInt());
  }

  /**
   * @param schema The Avro schema where the specified fields should be removed from.
   * @return A new Avro schema with the specified fields removed.
   * @throws SchemaConversionException When specified fields do not exist in the Schema.
   */
  public Schema removeFieldsStrictly(Schema schema) throws SchemaConversionException {
    MutableInt count = new MutableInt();
    Schema removed = removeFields(schema, Maps.<String, Schema> newHashMap(), count);
    if (fieldsToRemove.size() != count.getValue()) {
      throw new SchemaConversionException("Failed to delete fields from schema. Fields to remove: " + fieldsToRemove +
                                          "Removed " + count.getValue() + " fields from Schema " + schema);
    }
    return removed;
  }

  private Schema removeFields(Schema schema, Map<String, Schema> schemaMap, MutableInt count) {

    switch (schema.getType()) {
      case RECORD:
        if (schemaMap.containsKey(schema.getFullName())) {
          return schemaMap.get(schema.getFullName());
        } else {
          return this.removeFieldsFromRecords(schema, schemaMap, count);
        }
      case UNION:
        return this.removeFieldsFromUnion(schema, schemaMap, count);
      case ARRAY:
        return this.removeFieldsFromArray(schema, schemaMap, count);
      case MAP:
        return this.removeFieldsFromMap(schema, schemaMap, count);
      default:
        return schema;
    }
  }

  private Schema removeFieldsFromRecords(Schema schema, Map<String, Schema> schemaMap, MutableInt count) {

    Schema newRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

    // Put an incomplete schema into schemaMap to avoid re-processing a recursive field.
    // The fields in the incomplete schema will be populated once the current schema is completely processed.
    schemaMap.put(schema.getFullName(), newRecord);

    List<Field> newFields = Lists.newArrayList();
    for (Field field : schema.getFields()) {
      if (!this.shouldRemove(field)) {
        Field newField;
        if (this.children.containsKey(field.name())) {
          newField = new Field(field.name(), this.children.get(field.name()).removeFields(field.schema(), schemaMap, count),
              field.doc(), field.defaultValue());
        } else {
          newField = new Field(field.name(), DO_NOTHING_INSTANCE.removeFields(field.schema(), schemaMap, count), field.doc(),
              field.defaultValue());
        }
        newFields.add(newField);
      } else {
        count.increment();
      }
    }

    newRecord.setFields(newFields);
    return newRecord;
  }

  private boolean shouldRemove(Field field) {

    // A field should be removed if it is the last component in a specified field name,
    // e.g., "memberId" in "header.memberId".
    return this.children.containsKey(field.name()) && this.children.get(field.name()).children.isEmpty();
  }

  private Schema removeFieldsFromUnion(Schema schema, Map<String, Schema> schemaMap, MutableInt count) {
    List<Schema> newUnion = Lists.newArrayList();
    for (Schema unionType : schema.getTypes()) {
      newUnion.add(this.removeFields(unionType, schemaMap, count));
    }
    return Schema.createUnion(newUnion);
  }

  private Schema removeFieldsFromArray(Schema schema, Map<String, Schema> schemaMap, MutableInt count) {
    return Schema.createArray(this.removeFields(schema.getElementType(), schemaMap, count));
  }

  private Schema removeFieldsFromMap(Schema schema, Map<String, Schema> schemaMap, MutableInt count) {
    return Schema.createMap(this.removeFields(schema.getValueType(), schemaMap, count));
  }
}
