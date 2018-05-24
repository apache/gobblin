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

package org.apache.gobblin.converter.filter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.AvroToAvroConverterBase;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.converter.SingleRecordIterable;
import org.apache.gobblin.util.AvroUtils;


/**
 * Converts schema and data by choosing only selected fields provided by user.
 */
public class AvroFieldsPickConverter extends AvroToAvroConverterBase {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFieldsPickConverter.class);

  private static final Splitter SPLITTER_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Splitter SPLITTER_ON_DOT = Splitter.on('.').trimResults().omitEmptyStrings();

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
   * @see org.apache.gobblin.converter.AvroToAvroConverterBase#convertSchema(org.apache.avro.Schema, org.apache.gobblin.configuration.WorkUnitState)
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    LOG.info("Converting schema " + inputSchema);
    String fieldsStr = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS);
    Preconditions.checkNotNull(fieldsStr, ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS
        + " is required for converter " + this.getClass().getSimpleName());
    LOG.info("Converting schema to selected fields: " + fieldsStr);

    try {
      return createSchema(inputSchema, fieldsStr);
    } catch (Exception e) {
      throw new SchemaConversionException(e);
    }
  }

  /**
   * Creates Schema containing only specified fields.
   *
   * Traversing via either fully qualified names or input Schema is quite inefficient as it's hard to align each other.
   * Also, as Schema's fields is immutable, all the fields need to be collected before updating field in Schema. Figuring out all
   * required field in just input Schema and fully qualified names is also not efficient as well.
   *
   * This is where Trie comes into picture. Having fully qualified names in Trie means, it is aligned with input schema and also it can
   * provides all children on specific prefix. This solves two problems mentioned above.
   *
   * 1. Based on fully qualified field name, build a Trie to present dependencies.
   * 2. Traverse the Trie. If it's leaf, add field. If it's not a leaf, recurse with child schema.
   *
   * @param schema
   * @param fieldsStr
   * @return
   */
  private static Schema createSchema(Schema schema, String fieldsStr) {
    List<String> fields = SPLITTER_ON_COMMA.splitToList(fieldsStr);
    TrieNode root = buildTrie(fields);
    return createSchemaHelper(schema, root);
  }

  private static Schema createSchemaHelper(final Schema inputSchema, TrieNode node) {
    List<Field> newFields = Lists.newArrayList();
    for (TrieNode child : node.children.values()) {
      Schema recordSchema = getActualRecord(inputSchema);
      Field innerSrcField = recordSchema.getField(child.val);
      Preconditions.checkNotNull(innerSrcField, child.val + " does not exist under " + recordSchema);

      if (child.children.isEmpty()) { //Leaf
        newFields.add(
            new Field(innerSrcField.name(), innerSrcField.schema(), innerSrcField.doc(), innerSrcField.defaultValue()));
      } else {
        Schema innerSrcSchema = innerSrcField.schema();

        Schema innerDestSchema = createSchemaHelper(innerSrcSchema, child); //Recurse of schema
        Field innerDestField =
            new Field(innerSrcField.name(), innerDestSchema, innerSrcField.doc(), innerSrcField.defaultValue());
        newFields.add(innerDestField);
      }
    }

    if (Type.UNION.equals(inputSchema.getType())) {
      Preconditions.checkArgument(inputSchema.getTypes().size() <= 2,
          "For union type in nested record, it should only have NULL and Record type");

      Schema recordSchema = getActualRecord(inputSchema);
      Schema newRecord = Schema.createRecord(recordSchema.getName(), recordSchema.getDoc(), recordSchema.getNamespace(),
          recordSchema.isError());
      newRecord.setFields(newFields);
      if (inputSchema.getTypes().size() == 1) {
        return Schema.createUnion(newRecord);
      }
      return Schema.createUnion(Lists.newArrayList(Schema.create(Type.NULL), newRecord));
    }

    Schema newRecord = Schema.createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(),
        inputSchema.isError());
    newRecord.setFields(newFields);
    return newRecord;
  }

  /**
   * For the schema that is a UNION type with NULL and Record type, it provides Records type.
   * @param inputSchema
   * @return
   */
  private static Schema getActualRecord(Schema inputSchema) {
    if (Type.RECORD.equals(inputSchema.getType())) {
      return inputSchema;
    }

    Preconditions.checkArgument(Type.UNION.equals(inputSchema.getType()), "Nested schema is only support with either record or union type of null with record");
    Preconditions.checkArgument(inputSchema.getTypes().size() <= 2,
        "For union type in nested record, it should only have NULL and Record type");

    for (Schema inner : inputSchema.getTypes()) {
      if (Type.NULL.equals(inner.getType())) {
        continue;
      }
      Preconditions.checkArgument(Type.RECORD.equals(inner.getType()), "For union type in nested record, it should only have NULL and Record type");
      return inner;

    }
    throw new IllegalArgumentException(inputSchema + " is not supported.");
  }

  private static TrieNode buildTrie(List<String> fqns) {
    TrieNode root = new TrieNode(null);
    for (String fqn : fqns) {
      root.add(fqn);
    }
    return root;
  }

  private static class TrieNode {
    private String val;
    private Map<String, TrieNode> children;

    TrieNode(String val) {
      this.val = val;
      this.children = Maps.newLinkedHashMap();
    }

    void add(String fqn) {
      addHelper(this, SPLITTER_ON_DOT.splitToList(fqn).iterator(), fqn);
    }

    void addHelper(TrieNode node, Iterator<String> fqnIterator, String fqn) {
      if (!fqnIterator.hasNext()) {
        return;
      }

      String val = fqnIterator.next();
      TrieNode child = node.children.get(val);
      if (child == null) {
        child = new TrieNode(val);
        node.children.put(val, child);
      } else if (!fqnIterator.hasNext()) {
        //Leaf but there's existing record
        throw new IllegalArgumentException("Duplicate record detected: " + fqn);
      }
      addHelper(child, fqnIterator, fqn);
    }

    @Override
    public String toString() {
      return "[val: " + this.val + " , children: " + this.children.values() + " ]";
    }
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable<>(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }
}
