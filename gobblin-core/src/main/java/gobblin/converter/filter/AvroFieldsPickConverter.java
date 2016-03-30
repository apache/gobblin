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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;

/**
 * Converts schema and data by choosing only selected fields provided by user.
 */
public class AvroFieldsPickConverter extends AvroToAvroConverterBase {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFieldsPickConverter.class);

  private static final Splitter SPLITTER_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Joiner JOINER_ON_DOT = Joiner.on('.');
  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  /**
   * Convert the schema to contain only specified field. This will reuse AvroSchemaFieldRemover by listing fields not specified and remove it
   * from the schema
   * 1. Retrieve list of fields from property
   * 2. Traverse schema and get list of fields to be removed
   * 3. While traversing also confirm specified fields from property also exist
   * 4. Convert schema by using AvroSchemaFieldRemover
   *
   * Each Avro Record type increments depth and from input depth is represented by '.'. Avro schema is always expected to start with Record type
   * and first record type is depth 0 and won't be represented by '.'. As it's always expected to start with Record type, it's not necessary to disambiguate.
   * After first record type, if it reaches another record type, the prefix of the field name will be
   * "[Record name].".
   *
   * Example:
   * {
          "namespace": "example.avro",
          "type": "record",
          "name": "user",
          "fields": [
            {
              "name": "name",
              "type": "string"
            },
            {
              "name": "favorite_number",
              "type": [
                "int",
                "null"
              ]
            },
            {
              "type": "record",
              "name": "address",
              "fields": [
                {
                  "name": "city",
                  "type": "string"
                }
              ]
            }
          ]
        }
   * If user wants to only choose name and city, the input parameter should be "name,address.city". Note that it is not user.name as first record is depth zero.
   * {@inheritDoc}
   * @see gobblin.converter.AvroToAvroConverterBase#convertSchema(org.apache.avro.Schema, gobblin.configuration.WorkUnitState)
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    LOG.info("Converting schema " + inputSchema);
    String fieldsStr = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS);
    Preconditions.checkNotNull(fieldsStr, ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS
                                   + " is required for converter " + this.getClass().getSimpleName());
    LOG.info("Converting schema to selected fields: " + fieldsStr);

    List<String> fields = SPLITTER_ON_COMMA.splitToList(fieldsStr);
    Set<String> fieldsToRemove = fieldsToRemove(inputSchema, fields);
    LOG.info("Fields to be removed from schema: " + fieldsToRemove);
    AvroSchemaFieldRemover remover = new AvroSchemaFieldRemover(JOINER_ON_COMMA.join(fieldsToRemove));
    Schema converted = remover.removeFieldsStrictly(inputSchema);
    LOG.info("Converted schema: " + converted);
    return converted;
  }

  /**
   * Provides fields to be removed based on required field names.
   * @param schema
   * @param requiredFields Fields chosen to be remained in Schema from user.
   * @return Field names that needs to be removed from Schema
   * @throws SchemaConversionException If first entry of Avro is not a Record type or if required field(s) does not exist in the Schema.
   */
  private Set<String> fieldsToRemove(Schema schema, Collection<String> requiredFields) throws SchemaConversionException {
    Set<String> copiedRequiredFields = Sets.newHashSet(requiredFields);
    Set<String> fieldsToRemove = Sets.newHashSet();

    if(!Type.RECORD.equals(schema.getType())) {
      throw new SchemaConversionException("First entry of Avro schema should be a Record type " + schema);
    }
    LinkedList<String> fullyQualifiedName = Lists.<String>newLinkedList();
    for (Field f : schema.getFields()) {
      fieldsToRemoveHelper(f.schema(), f, copiedRequiredFields, fullyQualifiedName, fieldsToRemove);
    }
    if (!copiedRequiredFields.isEmpty()) {
      throw new SchemaConversionException("Failed to pick field(s) as some field(s) " + copiedRequiredFields + " does not exist in Schema " + schema);
    }
    return fieldsToRemove;
  }

  /**
   * Helper method for fieldsToRemove. Note that it mutates requiredFields parameter by removing elements when selected
   * @param schema
   * @param field
   * @param requiredFields Note that it mutates this parameter by removing elements. If user provided fields exist in the Schema this parameter ends up empty.
   * @param fqn fully qualified name
   * @param fieldsToRemove
   * @return
   * @throws SchemaConversionException When Avro schema has duplicate field name
   */
  private Set<String> fieldsToRemoveHelper(Schema schema, Field field, Set<String> requiredFields, LinkedList<String> fqn, Set<String> fieldsToRemove) throws SchemaConversionException {
    if(Type.RECORD.equals(schema.getType())) { //Add name of record into fqn and recurse
      fqn.addLast(schema.getName());
      for (Field f : schema.getFields()) {
        fieldsToRemoveHelper(f.schema(), f, requiredFields, fqn, fieldsToRemove);
      }
      fqn.removeLast();
      return fieldsToRemove;
    }

    fqn.addLast(field.name());
    String fqnStr = JOINER_ON_DOT.join(fqn);
    boolean isRequiredField = requiredFields.remove(fqnStr);

    if(!isRequiredField) {
      boolean isFirstRemoval = fieldsToRemove.add(fqnStr);
      if (!isFirstRemoval) {
        throw new SchemaConversionException("Duplicate name " + fqnStr + " is not allowed");
      }
    }
    fqn.removeLast();
    return fieldsToRemove;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable<GenericRecord>(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }
}
