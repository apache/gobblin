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

package gobblin.converter.avro.replacer;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.ClassAliasResolver;
import gobblin.util.ForkOperatorUtils;


/**
 * Replaces string fields in avro records.
 *
 * Usage: this class reads the fields to be replaced from configuration keys of the form:
 * {@link #REPLACE_FIELD_KEY}.<my-field> = <replacer-alias>[:<parameters>]
 * For example, to replace the field "myField" with the constant "myValue", use
 * {@link #REPLACE_FIELD_KEY}.myField = constant:myValue
 *
 * {@link AvroFieldReplacer} can use any {@link StringValueReplacer} specified by alias by the user. For simplicity,
 * some {@link StringValueReplacer} take a single string argument which is passed in the same configuration key separated
 * by a ":" from the replacer-alias.
 */
public class AvroFieldReplacer extends Converter<Schema, Schema, GenericRecord, GenericRecord>{

  public static final String REPLACE_FIELD_KEY = "converter.avro.replacer.field";

  private static final Splitter SPLITTER = Splitter.on(":").limit(2);

  private final Map<String, StringValueReplacer> replacementMap = Maps.newHashMap();

  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    try {
      String fieldPathKey = ForkOperatorUtils.getPropertyNameForBranch(workUnit, REPLACE_FIELD_KEY);

      ClassAliasResolver<StringValueReplacer.ReplacerFactory> resolver = new ClassAliasResolver<>(StringValueReplacer.ReplacerFactory.class);
      Properties properties = workUnit.getProperties();

      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        if (entry.getKey() instanceof String && entry.getValue() instanceof String && ((String) entry.getKey()).startsWith(fieldPathKey)) {
          String fieldName = ((String) entry.getKey()).substring(fieldPathKey.length() + 1);
          Iterator<String> it = SPLITTER.split((String) entry.getValue()).iterator();
          StringValueReplacer.ReplacerFactory factory = resolver.resolveClass(it.next()).newInstance();
          this.replacementMap.put(fieldName, factory.buildReplacer(it.hasNext() ? it.next() : "", properties));
        }
      }

      return inputSchema;
    } catch (ReflectiveOperationException roe) {
      throw new SchemaConversionException("Failed to instantiate " + AvroFieldReplacer.class, roe);
    }
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    for (Map.Entry<String, StringValueReplacer> entry : this.replacementMap.entrySet()) {
      Object currentValue = inputRecord.get(entry.getKey());
      if (currentValue != null && currentValue instanceof String) {
        inputRecord.put(entry.getKey(), entry.getValue().replace((String) inputRecord.get(entry.getKey())));
      }
    }
    return new SingleRecordIterable<>(inputRecord);
  }
}
