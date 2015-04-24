/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import static org.apache.avro.SchemaCompatibility.*;
import static org.apache.avro.SchemaCompatibility.SchemaCompatibilityType.*;


/**
 * A Utils class for dealing with Avro objects
 */
public class AvroUtils {

  private static final String FIELD_LOCATION_DELIMITER = ".";

  /**
   * Given a GenericRecord, this method will return the schema of the field specified by the path parameter. The
   * fieldLocation parameter is an ordered string specifying the location of the nested field to retrieve. For example,
   * field1.nestedField1 takes the the schema of the field "field1", and retrieves the schema "nestedField1" from it.
   * @param schema is the record to retrieve the schema from
   * @param fieldLocation is the location of the field
   * @return the schema of the field
   */
  public static Optional<Schema> getFieldSchema(Schema schema, String fieldLocation) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldLocation));

    Splitter splitter = Splitter.on(FIELD_LOCATION_DELIMITER).omitEmptyStrings().trimResults();
    List<String> pathList = Lists.newArrayList(splitter.split(fieldLocation));

    if (pathList.size() == 0) {
      return Optional.absent();
    }

    return AvroUtils.getFieldSchemaHelper(schema, pathList, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getFieldSchema(Schema, String)}
   * @param schema passed from {@link #getFieldValue(Schema, String)}
   * @param pathList passed from {@link #getFieldValue(Schema, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the schema of the field
   */
  private static Optional<Schema> getFieldSchemaHelper(Schema schema, List<String> pathList, int field) {
    if ((field + 1) == pathList.size()) {
      return Optional.fromNullable(schema.getField(pathList.get(field)).schema());
    } else {
      return AvroUtils.getFieldSchemaHelper(schema.getField(pathList.get(field)).schema(), pathList, ++field);
    }
  }

  /**
   * Given a GenericRecord, this method will return the field specified by the path parameter. The fieldLocation
   * parameter is an ordered string specifying the location of the nested field to retrieve. For example,
   * field1.nestedField1 takes the the value of the field "field1", and retrieves the field "nestedField1" from it.
   * @param record is the record to retrieve the field from
   * @param fieldLocation is the location of the field
   * @return the value of the field
   */
  public static Optional<Object> getFieldValue(GenericRecord record, String fieldLocation) {
    Preconditions.checkNotNull(record);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldLocation));

    Splitter splitter = Splitter.on(FIELD_LOCATION_DELIMITER).omitEmptyStrings().trimResults();
    List<String> pathList = splitter.splitToList(fieldLocation);

    if (pathList.size() == 0) {
      return Optional.absent();
    }

    return AvroUtils.getFieldHelper(record, pathList, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getFieldValue(GenericRecord, String)}
   * @param data passed from {@link #getFieldValue(GenericRecord, String)}
   * @param pathList passed from {@link #getFieldValue(GenericRecord, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the value of the field
   */
  private static Optional<Object> getFieldHelper(Object data, List<String> pathList, int field) {
    if (data == null) {
      return Optional.absent();
    }

    if ((field + 1) == pathList.size()) {
      return Optional.fromNullable(((Record) data).get(pathList.get(field)));
    } else {
      return AvroUtils.getFieldHelper(((Record) data).get(pathList.get(field)), pathList, ++field);
    }
  }

  /**
   * Change the schema of an Avro record.
   * @param record The Avro record whose schema is to be changed.
   * @param newSchema The target schema. It must be compatible as reader schema with record.getSchema() as writer schema.
   * @return a new Avro record with the new schema.
   * @throws IOException if conversion failed.
   */
  public static GenericRecord convertRecordSchema(GenericRecord record, Schema newSchema) throws IOException {
    Preconditions.checkArgument(checkReaderWriterCompatibility(newSchema, record.getSchema()).getType() == COMPATIBLE);

    Closer closer = Closer.create();
    try {
      ByteArrayOutputStream out = closer.register(new ByteArrayOutputStream());
      Encoder encoder = new EncoderFactory().directBinaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());
      writer.write(record, encoder);
      BinaryDecoder decoder = new DecoderFactory().binaryDecoder(out.toByteArray(), null);
      DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(record.getSchema(), newSchema);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new IOException(String.format(
          "Cannot convert avro record to new schema. Origianl schema = %s, new schema = %s", record.getSchema(),
          newSchema), e);
    } finally {
      closer.close();
    }
  }
}
