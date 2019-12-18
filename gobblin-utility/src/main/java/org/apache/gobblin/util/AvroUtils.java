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

package org.apache.gobblin.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * A Utils class for dealing with Avro objects
 */
@Slf4j
public class AvroUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  public static final String FIELD_LOCATION_DELIMITER = ".";

  private static final String AVRO_SUFFIX = ".avro";

  /**
   * Validates that the provided reader schema can be used to decode avro data written with the
   * provided writer schema.
   * @param readerSchema schema to check.
   * @param writerSchema schema to check.
   * @param ignoreNamespace whether name and namespace should be ignored in validation
   * @return true if validation passes
   */
  public static boolean checkReaderWriterCompatibility(Schema readerSchema, Schema writerSchema, boolean ignoreNamespace) {
    if (ignoreNamespace) {
      List<Schema.Field> fields = deepCopySchemaFields(readerSchema);
      readerSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(),
          readerSchema.isError());
      readerSchema.setFields(fields);
    }

    return SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema).getType().equals(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
  }

  public static List<Field> deepCopySchemaFields(Schema readerSchema) {
    return readerSchema.getFields().stream()
        .map(field -> {
          Field f = new Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order());
          field.getProps().forEach((key, value) -> f.addProp(key, value));
          return f;
        })
        .collect(Collectors.toList());
  }


  public static class AvroPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(AVRO_SUFFIX);
    }
  }

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
   * @param schema passed from {@link #getFieldSchema(Schema, String)}
   * @param pathList passed from {@link #getFieldSchema(Schema, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the schema of the field
   */
  private static Optional<Schema> getFieldSchemaHelper(Schema schema, List<String> pathList, int field) {
    if (schema.getType() == Type.RECORD && schema.getField(pathList.get(field)) == null) {
      return Optional.absent();
    }
    switch (schema.getType()) {
      case UNION:
        if (AvroSerdeUtils.isNullableType(schema)) {
          return AvroUtils.getFieldSchemaHelper(AvroSerdeUtils.getOtherTypeFromNullableType(schema), pathList, field);
        }
        throw new AvroRuntimeException("Union of complex types cannot be handled : " + schema);
      case MAP:
        if ((field + 1) == pathList.size()) {
          return Optional.fromNullable(schema.getValueType());
        }
        return AvroUtils.getFieldSchemaHelper(schema.getValueType(), pathList, ++field);
      case RECORD:
        if ((field + 1) == pathList.size()) {
          return Optional.fromNullable(schema.getField(pathList.get(field)).schema());
        }
        return AvroUtils.getFieldSchemaHelper(schema.getField(pathList.get(field)).schema(), pathList, ++field);
      default:
        throw new AvroRuntimeException("Invalid type in schema : " + schema);
    }
  }

  /**
   * Given a GenericRecord, this method will return the field specified by the path parameter. The
   * fieldLocation parameter is an ordered string specifying the location of the nested field to retrieve. For example,
   * field1.nestedField1 takes field "field1", and retrieves "nestedField1" from it.
   * @param schema is the record to retrieve the schema from
   * @param fieldLocation is the location of the field
   * @return the field
   */
  public static Optional<Field> getField(Schema schema, String fieldLocation) {
    Preconditions.checkNotNull(schema);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldLocation));

    Splitter splitter = Splitter.on(FIELD_LOCATION_DELIMITER).omitEmptyStrings().trimResults();
    List<String> pathList = Lists.newArrayList(splitter.split(fieldLocation));

    if (pathList.size() == 0) {
      return Optional.absent();
    }

    return AvroUtils.getFieldHelper(schema, pathList, 0);
  }

  /**
   * Helper method that does the actual work for {@link #getField(Schema, String)}
   * @param schema passed from {@link #getFieldSchema(Schema, String)}
   * @param pathList passed from {@link #getFieldSchema(Schema, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the field
   */
  private static Optional<Field> getFieldHelper(Schema schema, List<String> pathList, int field) {
    Field curField = schema.getField(pathList.get(field));
    if (field + 1 == pathList.size()) {
      return Optional.fromNullable(curField);
    }

    Schema fieldSchema = curField.schema();
    switch (fieldSchema.getType()) {
      case UNION:
        throw new AvroRuntimeException("Union of complex types cannot be handled : " + schema);
      case MAP:
        return AvroUtils.getFieldHelper(fieldSchema.getValueType(), pathList, ++field);
      case RECORD:
        return AvroUtils.getFieldHelper(fieldSchema, pathList, ++field);
      case ARRAY:
        return AvroUtils.getFieldHelper(fieldSchema.getElementType(), pathList, ++field);
      default:
        throw new AvroRuntimeException("Invalid type " + fieldSchema.getType() + " in schema : " + schema);
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
    Map<String, Object> ret = getMultiFieldValue(record, fieldLocation);
    return Optional.fromNullable(ret.get(fieldLocation));
  }

  public static Map<String, Object> getMultiFieldValue(GenericRecord record, String fieldLocation) {
    Preconditions.checkNotNull(record);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldLocation));

    Splitter splitter = Splitter.on(FIELD_LOCATION_DELIMITER).omitEmptyStrings().trimResults();
    List<String> pathList = splitter.splitToList(fieldLocation);

    if (pathList.size() == 0) {
      return Collections.emptyMap();
    }

    HashMap<String, Object> retVal = new HashMap<String, Object>();
    AvroUtils.getFieldHelper(retVal, record, pathList, 0);
    return retVal;
  }

  /**
   * Helper method that does the actual work for {@link #getFieldValue(GenericRecord, String)}
   * @param data passed from {@link #getFieldValue(GenericRecord, String)}
   * @param pathList passed from {@link #getFieldValue(GenericRecord, String)}
   * @param field keeps track of the index used to access the list pathList
   * @return the value of the field
   */
  private static void getFieldHelper(Map<String, Object> retVal,
      Object data, List<String> pathList, int field) {
    if (data == null) {
      return;
    }

    if ((field + 1) == pathList.size()) {
      Object val = null;
      Joiner joiner = Joiner.on(".");
      String key = joiner.join(pathList.iterator());

      if (data instanceof Map) {
        val = getObjectFromMap((Map)data, pathList.get(field));
      } else if (data instanceof List) {
        val = getObjectFromArray((List)data, Integer.parseInt(pathList.get(field)));
      } else {
        val = ((GenericRecord)data).get(pathList.get(field));
      }

      if (val != null) {
        retVal.put(key, val);
      }

      return;
    }
    if (data instanceof Map) {
      AvroUtils.getFieldHelper(retVal, getObjectFromMap((Map) data, pathList.get(field)), pathList, ++field);
      return;
    }
    if (data instanceof List) {
      if (pathList.get(field).trim().equals("*")) {
        List arr = (List)data;
        Iterator it = arr.iterator();
        int i = 0;
        while (it.hasNext()) {
          Object val = it.next();
          List<String> newPathList = new ArrayList<>(pathList);
          newPathList.set(field, String.valueOf(i));
          AvroUtils.getFieldHelper(retVal, val, newPathList, field + 1);
          i++;
        }
      } else {
        AvroUtils
            .getFieldHelper(retVal, getObjectFromArray((List) data, Integer.parseInt(pathList.get(field))), pathList, ++field);
      }
      return;
    }

    AvroUtils.getFieldHelper(retVal, ((GenericRecord) data).get(pathList.get(field)), pathList, ++field);
    return;
  }

  /**
   * Given a map: key -> value, return a map: key.toString() -> value.toString(). Avro serializer wraps a String
   * into {@link Utf8}. This method helps to restore the original string map object
   *
   * @param map a map object
   * @return a map of strings
   */
  @SuppressWarnings("unchecked")
  public static Map<String, String> toStringMap(Object map) {
    if (map == null) {
      return null;
    }

    if (map instanceof Map) {
      Map<Object, Object> rawMap = (Map<Object, Object>) map;
      Map<String, String> stringMap = new HashMap<>();
      for (Entry<Object, Object> entry : rawMap.entrySet()) {
        stringMap.put(entry.getKey().toString(), entry.getValue().toString());
      }
      return stringMap;
    } else {
      throw new AvroRuntimeException("value must be a map");
    }
  }

  /**
   * This method is to get object from map given a key as string.
   * Avro persists string as Utf8
   * @param map passed from {@link #getFieldHelper(Map, Object, List, int)}
   * @param key passed from {@link #getFieldHelper(Map, Object, List, int)}
   * @return This could again be a GenericRecord
   */

  private static Object getObjectFromMap(Map map, String key) {
    Utf8 utf8Key = new Utf8(key);
    Object value = map.get(utf8Key);
    if (value == null) {
      return map.get(key);
    }

    return value;
  }

  /**
   * Get an object from an array given an index.
   */
  private static Object getObjectFromArray(List array, int index) {
    return array.get(index);
  }

  /**
   * Change the schema of an Avro record.
   * @param record The Avro record whose schema is to be changed.
   * @param newSchema The target schema. It must be compatible as reader schema with record.getSchema() as writer schema.
   * @return a new Avro record with the new schema.
   * @throws IOException if conversion failed.
   */
  public static GenericRecord convertRecordSchema(GenericRecord record, Schema newSchema) throws IOException {
    if (record.getSchema().equals(newSchema)) {
      return record;
    }

    try {
      BinaryDecoder decoder = new DecoderFactory().binaryDecoder(recordToByteArray(record), null);
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(record.getSchema(), newSchema);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new IOException(
          String.format("Cannot convert avro record to new schema. Original schema = %s, new schema = %s",
              record.getSchema(), newSchema),
          e);
    }
  }

  /**
   * Convert a GenericRecord to a byte array.
   */
  public static byte[] recordToByteArray(GenericRecord record) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
      writer.write(record, encoder);
      byte[] byteArray = out.toByteArray();
      return byteArray;
    }
  }

  /**
   * Get Avro schema from an Avro data file.
   */
  public static Schema getSchemaFromDataFile(Path dataFile, FileSystem fs) throws IOException {
    try (SeekableInput sin = new FsInput(dataFile, fs.getConf());
        DataFileReader<GenericRecord> reader = new DataFileReader<>(sin, new GenericDatumReader<GenericRecord>())) {
      return reader.getSchema();
    }
  }

  /**
   * Parse Avro schema from a schema file.
   */
  public static Schema parseSchemaFromFile(Path filePath, FileSystem fs) throws IOException {
    Preconditions.checkArgument(fs.exists(filePath), filePath + " does not exist");

    try (FSDataInputStream in = fs.open(filePath)) {
      return new Schema.Parser().parse(in);
    }
  }

  public static void writeSchemaToFile(Schema schema, Path filePath, FileSystem fs, boolean overwrite)
      throws IOException {
    writeSchemaToFile(schema, filePath, null, fs, overwrite);
  }

  public static void writeSchemaToFile(Schema schema, Path filePath, Path tempFilePath, FileSystem fs, boolean overwrite)
      throws IOException {
    writeSchemaToFile(schema, filePath, tempFilePath, fs, overwrite, new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.READ));
  }

  public static void writeSchemaToFile(Schema schema, Path filePath, FileSystem fs, boolean overwrite, FsPermission perm)
    throws IOException {
    writeSchemaToFile(schema, filePath, null, fs, overwrite, perm);
  }

  /**
   * Write a schema to a file
   * @param schema the schema
   * @param filePath the target file
   * @param tempFilePath if not null then this path is used for a temporary file used to stage the write
   * @param fs a {@link FileSystem}
   * @param overwrite should any existing target file be overwritten?
   * @param perm permissions
   * @throws IOException
   */
  public static void writeSchemaToFile(Schema schema, Path filePath, Path tempFilePath, FileSystem fs, boolean overwrite,
      FsPermission perm)
      throws IOException {
    boolean fileExists = fs.exists(filePath);

    if (!overwrite) {
      Preconditions.checkState(!fileExists, filePath + " already exists");
    } else {
      // delete the target file now if not using a staging file
      if (fileExists && null == tempFilePath) {
        HadoopUtils.deletePath(fs, filePath, true);
        // file has been removed
        fileExists = false;
      }
    }

    // If the file exists then write to a temp file to make the replacement as close to atomic as possible
    Path writeFilePath = fileExists ? tempFilePath : filePath;

    try (DataOutputStream dos = fs.create(writeFilePath)) {
      dos.writeChars(schema.toString());
    }
    fs.setPermission(writeFilePath, perm);

    // Replace existing file with the staged file
    if (fileExists) {
      if (!fs.delete(filePath, true)) {
        throw new IOException(
            String.format("Failed to delete %s while renaming %s to %s", filePath, tempFilePath, filePath));
      }

      HadoopUtils.movePath(fs, tempFilePath, fs, filePath, true, fs.getConf());
    }
  }

  /**
   * Get the latest avro schema for a directory
   * @param directory the input dir that contains avro files
   * @param fs the {@link FileSystem} for the given directory.
   * @param latest true to return latest schema, false to return oldest schema
   * @return the latest/oldest schema in the directory
   * @throws IOException
   */
  public static Schema getDirectorySchema(Path directory, FileSystem fs, boolean latest) throws IOException {
    Schema schema = null;
    try (Closer closer = Closer.create()) {
      List<FileStatus> files = getDirectorySchemaHelper(directory, fs);
      if (files == null || files.size() == 0) {
        LOG.warn("There is no previous avro file in the directory: " + directory);
      } else {
        FileStatus file = latest ? files.get(0) : files.get(files.size() - 1);
        LOG.debug("Path to get the avro schema: " + file);
        FsInput fi = new FsInput(file.getPath(), fs.getConf());
        GenericDatumReader<GenericRecord> genReader = new GenericDatumReader<>();
        schema = closer.register(new DataFileReader<>(fi, genReader)).getSchema();
      }
    } catch (IOException ioe) {
      throw new IOException("Cannot get the schema for directory " + directory, ioe);
    }
    return schema;
  }

  /**
   * Get the latest avro schema for a directory
   * @param directory the input dir that contains avro files
   * @param conf configuration
   * @param latest true to return latest schema, false to return oldest schema
   * @return the latest/oldest schema in the directory
   * @throws IOException
   */
  public static Schema getDirectorySchema(Path directory, Configuration conf, boolean latest) throws IOException {
    return getDirectorySchema(directory, FileSystem.get(conf), latest);
  }

  private static List<FileStatus> getDirectorySchemaHelper(Path directory, FileSystem fs) throws IOException {
    List<FileStatus> files = Lists.newArrayList();
    if (fs.exists(directory)) {
      getAllNestedAvroFiles(fs.getFileStatus(directory), files, fs);
      if (files.size() > 0) {
        Collections.sort(files, FileListUtils.LATEST_MOD_TIME_ORDER);
      }
    }
    return files;
  }

  private static void getAllNestedAvroFiles(FileStatus dir, List<FileStatus> files, FileSystem fs) throws IOException {
    if (dir.isDirectory()) {
      FileStatus[] filesInDir = fs.listStatus(dir.getPath());
      if (filesInDir != null) {
        for (FileStatus f : filesInDir) {
          getAllNestedAvroFiles(f, files, fs);
        }
      }
    } else if (dir.getPath().getName().endsWith(AVRO_SUFFIX)) {
      files.add(dir);
    }
  }

  /**
   * Merge oldSchema and newSchame. Set a field default value to null, if this field exists in the old schema but not in the new schema.
   * @param oldSchema
   * @param newSchema
   * @return schema that contains all the fields in both old and new schema.
   */
  public static Schema nullifyFieldsForSchemaMerge(Schema oldSchema, Schema newSchema) {
    if (oldSchema == null) {
      LOG.warn("No previous schema available, use the new schema instead.");
      return newSchema;
    }

    if (!(oldSchema.getType().equals(Type.RECORD) && newSchema.getType().equals(Type.RECORD))) {
      LOG.warn("Both previous schema and new schema need to be record type. Quit merging schema.");
      return newSchema;
    }

    List<Field> combinedFields = Lists.newArrayList();
    for (Field newFld : newSchema.getFields()) {
      combinedFields.add(new Field(newFld.name(), newFld.schema(), newFld.doc(), newFld.defaultValue()));
    }

    for (Field oldFld : oldSchema.getFields()) {
      if (newSchema.getField(oldFld.name()) == null) {
        List<Schema> union = Lists.newArrayList();
        Schema oldFldSchema = oldFld.schema();
        if (oldFldSchema.getType().equals(Type.UNION)) {
          union.add(Schema.create(Type.NULL));
          for (Schema itemInUion : oldFldSchema.getTypes()) {
            if (!itemInUion.getType().equals(Type.NULL)) {
              union.add(itemInUion);
            }
          }
          Schema newFldSchema = Schema.createUnion(union);
          combinedFields.add(new Field(oldFld.name(), newFldSchema, oldFld.doc(), oldFld.defaultValue()));
        } else {
          union.add(Schema.create(Type.NULL));
          union.add(oldFldSchema);
          Schema newFldSchema = Schema.createUnion(union);
          combinedFields.add(new Field(oldFld.name(), newFldSchema, oldFld.doc(), oldFld.defaultValue()));
        }
      }
    }

    Schema mergedSchema =
        Schema.createRecord(newSchema.getName(), newSchema.getDoc(), newSchema.getNamespace(), newSchema.isError());
    mergedSchema.setFields(combinedFields);
    return mergedSchema;
  }

  /**
   * Remove map, array, enum fields, as well as union fields that contain map, array or enum,
   * from an Avro schema. A schema with these fields cannot be used as Mapper key in a
   * MapReduce job.
   */
  public static Optional<Schema> removeUncomparableFields(Schema schema) {
    return removeUncomparableFields(schema, Maps.newHashMap());
  }

  private static Optional<Schema> removeUncomparableFields(Schema schema, Map<Schema, Optional<Schema>> processed) {
    switch (schema.getType()) {
      case RECORD:
        return removeUncomparableFieldsFromRecord(schema, processed);
      case UNION:
        return removeUncomparableFieldsFromUnion(schema, processed);
      case MAP:
        return Optional.absent();
      case ARRAY:
        return Optional.absent();
      case ENUM:
        return Optional.absent();
      default:
        return Optional.of(schema);
    }
  }

  private static Optional<Schema> removeUncomparableFieldsFromRecord(Schema record, Map<Schema, Optional<Schema>> processed) {
    Preconditions.checkArgument(record.getType() == Schema.Type.RECORD);

    Optional<Schema> result = processed.get(record);
    if (null != result) {
      return result;
    }

    List<Field> fields = Lists.newArrayList();
    for (Field field : record.getFields()) {
      Optional<Schema> newFieldSchema = removeUncomparableFields(field.schema(), processed);
      if (newFieldSchema.isPresent()) {
        fields.add(new Field(field.name(), newFieldSchema.get(), field.doc(), field.defaultValue()));
      }
    }

    Schema newSchema = Schema.createRecord(record.getName(), record.getDoc(), record.getNamespace(), false);
    newSchema.setFields(fields);
    result = Optional.of(newSchema);
    processed.put(record, result);

    return result;
  }

  private static Optional<Schema> removeUncomparableFieldsFromUnion(Schema union, Map<Schema, Optional<Schema>> processed) {
    Preconditions.checkArgument(union.getType() == Schema.Type.UNION);

    Optional<Schema> result = processed.get(union);
    if (null != result) {
      return result;
    }

    List<Schema> newUnion = Lists.newArrayList();
    for (Schema unionType : union.getTypes()) {
      Optional<Schema> newType = removeUncomparableFields(unionType, processed);
      if (newType.isPresent()) {
        newUnion.add(newType.get());
      }
    }

    // Discard the union field if one or more types are removed from the union.
    if (newUnion.size() != union.getTypes().size()) {
      result = Optional.absent();
    } else {
      result = Optional.of(Schema.createUnion(newUnion));
    }
    processed.put(union, result);

    return result;
  }

  /**
   * Copies the input {@link org.apache.avro.Schema} but changes the schema name.
   * @param schema {@link org.apache.avro.Schema} to copy.
   * @param newName name for the copied {@link org.apache.avro.Schema}.
   * @return A {@link org.apache.avro.Schema} that is a copy of schema, but has the name newName.
   */
  public static Schema switchName(Schema schema, String newName) {
    if (schema.getName().equals(newName)) {
      return schema;
    }

    Schema newSchema = Schema.createRecord(newName, schema.getDoc(), schema.getNamespace(), schema.isError());

    List<Field> fields = schema.getFields();
    Iterable<Field> fieldsNew = Iterables.transform(fields, new Function<Field, Field>() {
      @Override
      public Schema.Field apply(Field input) {
        //this should never happen but the API has marked input as Nullable
        if (null == input) {
          return null;
        }
        Field field = new Field(input.name(), input.schema(), input.doc(), input.defaultValue(), input.order());
        return field;
      }
    });

    newSchema.setFields(Lists.newArrayList(fieldsNew));
    return newSchema;
  }

  /**
   * Copies the input {@link org.apache.avro.Schema} but changes the schema namespace.
   * @param schema {@link org.apache.avro.Schema} to copy.
   * @param namespaceOverride namespace for the copied {@link org.apache.avro.Schema}.
   * @return A {@link org.apache.avro.Schema} that is a copy of schema, but has the new namespace.
   */
  public static Schema switchNamespace(Schema schema, Map<String, String> namespaceOverride) {
    Schema newSchema;
    String newNamespace = StringUtils.EMPTY;

    // Process all Schema Types
    // (Primitives are simply cloned)
    switch (schema.getType()) {
      case ENUM:
        newNamespace = namespaceOverride.containsKey(schema.getNamespace()) ? namespaceOverride.get(schema.getNamespace())
            : schema.getNamespace();
        newSchema =
            Schema.createEnum(schema.getName(), schema.getDoc(), newNamespace, schema.getEnumSymbols());
        break;
      case FIXED:
        newNamespace = namespaceOverride.containsKey(schema.getNamespace()) ? namespaceOverride.get(schema.getNamespace())
            : schema.getNamespace();
        newSchema =
            Schema.createFixed(schema.getName(), schema.getDoc(), newNamespace, schema.getFixedSize());
        break;
      case MAP:
        newSchema = Schema.createMap(switchNamespace(schema.getValueType(), namespaceOverride));
        break;
      case RECORD:
        newNamespace = namespaceOverride.containsKey(schema.getNamespace()) ? namespaceOverride.get(schema.getNamespace())
            : schema.getNamespace();
        List<Schema.Field> newFields = new ArrayList<>();
        if (schema.getFields().size() > 0) {
          for (Schema.Field oldField : schema.getFields()) {
            Field newField = new Field(oldField.name(), switchNamespace(oldField.schema(), namespaceOverride), oldField.doc(),
                oldField.defaultValue(), oldField.order());
            newFields.add(newField);
          }
        }
        newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), newNamespace,
            schema.isError());
        newSchema.setFields(newFields);
        break;
      case UNION:
        List<Schema> newUnionMembers = new ArrayList<>();
        if (null != schema.getTypes() && schema.getTypes().size() > 0) {
          for (Schema oldUnionMember : schema.getTypes()) {
            newUnionMembers.add(switchNamespace(oldUnionMember, namespaceOverride));
          }
        }
        newSchema = Schema.createUnion(newUnionMembers);
        break;
      case ARRAY:
        newSchema = Schema.createArray(switchNamespace(schema.getElementType(), namespaceOverride));
        break;
      case BOOLEAN:
      case BYTES:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case NULL:
      case STRING:
        newSchema = Schema.create(schema.getType());
        break;
      default:
        String exceptionMessage = String.format("Schema namespace replacement failed for \"%s\" ", schema);
        LOG.error(exceptionMessage);

        throw new AvroRuntimeException(exceptionMessage);
    }

    // Copy schema metadata
    copyProperties(schema, newSchema);

    return newSchema;
  }

  /***
   * Copy properties from old Avro Schema to new Avro Schema
   * @param oldSchema Old Avro Schema to copy properties from
   * @param newSchema New Avro Schema to copy properties to
   */
  private static void copyProperties(Schema oldSchema, Schema newSchema) {
    Preconditions.checkNotNull(oldSchema);
    Preconditions.checkNotNull(newSchema);

    Map<String, JsonNode> props = oldSchema.getJsonProps();
    copyProperties(props, newSchema);
  }

  /***
   * Copy properties to an Avro Schema
   * @param props Properties to copy to Avro Schema
   * @param schema Avro Schema to copy properties to
   */
  private static void copyProperties(Map<String, JsonNode> props, Schema schema) {
    Preconditions.checkNotNull(schema);

    // (if null, don't copy but do not throw exception)
    if (null != props) {
      for (Map.Entry<String, JsonNode> prop : props.entrySet()) {
        schema.addProp(prop.getKey(), prop.getValue());
      }
    }
  }

  /**
   * Serialize a generic record as a relative {@link Path}. Useful for converting {@link GenericRecord} type keys
   * into file system locations. For example {field1=v1, field2=v2} returns field1=v1/field2=v2 if includeFieldNames
   * is true, or v1/v2 if it is false. Illegal HDFS tokens such as ':' and '\\' will be replaced with '_'.
   * Additionally, parameter replacePathSeparators controls whether to replace path separators ('/') with '_'.
   *
   * @param record {@link GenericRecord} to serialize.
   * @param includeFieldNames If true, each token in the path will be of the form key=value, otherwise, only the value
   *                          will be included.
   * @param replacePathSeparators If true, path separators ('/') in each token will be replaced with '_'.
   * @return A relative path where each level is a field in the input record.
   */
  public static Path serializeAsPath(GenericRecord record, boolean includeFieldNames, boolean replacePathSeparators) {
    if (record == null) {
      return new Path("");
    }
    List<String> tokens = Lists.newArrayList();
    for (Schema.Field field : record.getSchema().getFields()) {
      String sanitizedName = HadoopUtils.sanitizePath(field.name(), "_");
      String sanitizedValue = HadoopUtils.sanitizePath(record.get(field.name()).toString(), "_");
      if (replacePathSeparators) {
        sanitizedName = sanitizedName.replaceAll(Path.SEPARATOR, "_");
        sanitizedValue = sanitizedValue.replaceAll(Path.SEPARATOR, "_");
      }
      if (includeFieldNames) {
        tokens.add(String.format("%s=%s", sanitizedName, sanitizedValue));
      } else if (!Strings.isNullOrEmpty(sanitizedValue)) {
        tokens.add(sanitizedValue);
      }
    }
    return new Path(Joiner.on(Path.SEPARATOR).join(tokens));
  }

  /**
   * Escaping ";" and "'" character in the schema string when it is being used in DDL.
   * These characters are not allowed to show as part of column name but could possibly appear in documentation field.
   * Therefore the escaping behavior won't cause correctness issues.
   */
  public static String sanitizeSchemaString(String schemaString) {
    return schemaString.replaceAll(";",  "\\\\;").replaceAll("'", "\\\\'");
  }

  /**
   * Deserialize a {@link GenericRecord} from a byte array. This method is not intended for high performance.
   */
  public static GenericRecord slowDeserializeGenericRecord(byte[] serializedRecord, Schema schema) throws IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }

  /**
   * Decorate the {@link Schema} for a record with additional {@link Field}s.
   * @param inputSchema: must be a {@link Record} schema.
   * @return the decorated Schema. Fields are appended to the inputSchema.
   */
  public static Schema decorateRecordSchema(Schema inputSchema, @Nonnull List<Field> fieldList) {
    Preconditions.checkState(inputSchema.getType().equals(Type.RECORD));
    List<Field> outputFields = deepCopySchemaFields(inputSchema);
    List<Field> newOutputFields = Stream.concat(outputFields.stream(), fieldList.stream()).collect(Collectors.toList());

    Schema outputSchema = Schema.createRecord(inputSchema.getName(), inputSchema.getDoc(),
            inputSchema.getNamespace(), inputSchema.isError());
    outputSchema.setFields(newOutputFields);
    copyProperties(inputSchema, outputSchema);
    return outputSchema;
  }

  /**
   * Decorate a {@link GenericRecord} with additional fields and make it conform to an extended Schema
   * It is the caller's responsibility to ensure that the outputSchema is the merge of the inputRecord's schema
   * and the additional fields. The method does not check this for performance reasons, because it is expected to be called in the
   * critical path of processing a record.
   * Use {@link AvroUtils#decorateRecordSchema(Schema, List)} to generate such a Schema before calling this method.
   * @param inputRecord: record with data to be copied into the output record
   * @param fieldMap: values can be primitive types or GenericRecords if nested
   * @param outputSchema: the schema that the decoratedRecord will conform to
   * @return an outputRecord that contains a union of the fields in the inputRecord and the field-values in the fieldMap
   */
  public static GenericRecord decorateRecord(GenericRecord inputRecord, @Nonnull Map<String, Object> fieldMap,
          Schema outputSchema) {
    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    inputRecord.getSchema().getFields().forEach(f -> outputRecord.put(f.name(), inputRecord.get(f.name())));
    fieldMap.forEach((key, value) -> outputRecord.put(key, value));
    return outputRecord;
  }

  /**
   * Given a generic record, Override the name and namespace of the schema and return a new generic record
   * @param input input record who's name and namespace need to be overridden
   * @param nameOverride new name for the record schema
   * @param namespaceOverride Optional map containing namespace overrides
   * @return an output record with overridden name and possibly namespace
   */
  public static GenericRecord overrideNameAndNamespace(GenericRecord input, String nameOverride, Optional<Map<String, String>> namespaceOverride) {

    GenericRecord output = input;
    Schema newSchema = switchName(input.getSchema(), nameOverride);
    if(namespaceOverride.isPresent()) {
      newSchema = switchNamespace(newSchema, namespaceOverride.get());
    }

    try {
      output = convertRecordSchema(output, newSchema);
    } catch (Exception e){
      log.error("Unable to generate generic data record", e);
    }

    return output;
  }

  /**
   * Given a input schema, Override the name and namespace of the schema and return a new schema
   * @param input
   * @param nameOverride
   * @param namespaceOverride
   * @return a schema with overridden name and possibly namespace
   */
  public static Schema overrideNameAndNamespace(Schema input, String nameOverride, Optional<Map<String, String>> namespaceOverride) {

    Schema newSchema = switchName(input, nameOverride);
    if(namespaceOverride.isPresent()) {
      newSchema = switchNamespace(newSchema, namespaceOverride.get());
    }

    return newSchema;
  }

  @Builder
  @ToString
  public static class SchemaEntry {
    @Getter
    final String fieldName;
    final Schema schema;
    String fullyQualifiedType() {
      return schema.getFullName();
    }
  }

  /**
   * Check if a schema has recursive fields inside it
   * @param schema
   * @param logger : Optional logger if you want the method to log why it thinks the schema was recursive
   * @return true / false
   */
  public static boolean isSchemaRecursive(Schema schema, Optional<Logger> logger) {
    List<SchemaEntry> recursiveFields = new ArrayList<>();
    dropRecursive(new SchemaEntry(null, schema), Collections.EMPTY_LIST, recursiveFields);
    if (recursiveFields.isEmpty()) {
      return false;
    } else {
      if (logger.isPresent()) {
        logger.get().info("Found recursive fields [{}] in schema {}", recursiveFields.stream().map(f -> f.fieldName).collect(Collectors.joining(",")),
            schema.getFullName());
      }
      return true;
    }
  }


  /**
   * Drop recursive fields from a Schema. Recursive fields are fields that refer to types that are part of the
   * parent tree.
   * e.g. consider this Schema for a User
   * {
   *   "type": "record",
   *   "name": "User",
   *   "fields": [
   *     {"name": "name", "type": "string",
   *     {"name": "friend", "type": "User"}
   *   ]
   * }
   * the friend field is a recursive field. After recursion has been eliminated we expect the output Schema to look like
   * {
   *    "type": "record",
   *    "name": "User",
   *    "fields": [
   *    {"name": "name", "type": "string"}
   *    ]
   * }
   *
   * @param schema
   * @return a Pair of (The transformed schema with recursion eliminated, A list of @link{SchemaEntry} objects which
   * represent the fields that were removed from the original schema)
   */
  public static Pair<Schema, List<SchemaEntry>> dropRecursiveFields(Schema schema) {
    List<SchemaEntry> recursiveFields = new ArrayList<>();
    return new Pair(dropRecursive(new SchemaEntry(null, schema), Collections.EMPTY_LIST, recursiveFields),
        recursiveFields);
  }

  /**
   * Inner recursive method called by {@link #dropRecursiveFields(Schema)}
   * @param schemaEntry
   * @param parents
   * @param fieldsWithRecursion
   * @return the transformed Schema, null if schema is recursive w.r.t parent schema traversed so far
   */
  private static Schema dropRecursive(SchemaEntry schemaEntry, List<SchemaEntry> parents, List<SchemaEntry> fieldsWithRecursion) {
    Schema schema = schemaEntry.schema;
    // ignore primitive fields
    switch (schema.getType()) {
      case UNION:{
        List<Schema> unionTypes = schema.getTypes();
        List<Schema> copiedUnionTypes = new ArrayList<Schema>();
        for (Schema unionSchema: unionTypes) {
          SchemaEntry unionSchemaEntry = new SchemaEntry(
              schemaEntry.fieldName, unionSchema);
          copiedUnionTypes.add(dropRecursive(unionSchemaEntry, parents, fieldsWithRecursion));
        }
        if (copiedUnionTypes.stream().anyMatch(x -> x == null)) {
          // one or more types in the union are referring to a parent type (directly recursive),
          // entire union must be dropped
          return null;
        }
        else {
          Schema copySchema = Schema.createUnion(copiedUnionTypes);
          copyProperties(schema, copySchema);
          return copySchema;
        }
      }
      case RECORD:{
        // check if the type of this schema matches any in the parents list
        if (parents.stream().anyMatch(parent -> parent.fullyQualifiedType().equals(schemaEntry.fullyQualifiedType()))) {
          fieldsWithRecursion.add(schemaEntry);
          return null;
        }
        List<SchemaEntry> newParents = new ArrayList<>(parents);
        newParents.add(schemaEntry);
        List<Schema.Field> copiedSchemaFields = new ArrayList<>();
        for (Schema.Field field: schema.getFields()) {
          String fieldName = schemaEntry.fieldName != null ? schemaEntry.fieldName + "." + field.name() : field.name();
          SchemaEntry fieldSchemaEntry = new SchemaEntry(fieldName, field.schema());
          Schema copiedFieldSchema = dropRecursive(fieldSchemaEntry, newParents, fieldsWithRecursion);
          if (copiedFieldSchema == null) {
          } else {
            Schema.Field copiedField =
                new Schema.Field(field.name(), copiedFieldSchema, field.doc(), field.defaultValue(), field.order());
            copyFieldProperties(field, copiedField);
            copiedSchemaFields.add(copiedField);
          }
        }
        if (copiedSchemaFields.size() > 0) {
          Schema copiedRecord = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(),
              schema.isError());
          copiedRecord.setFields(copiedSchemaFields);
          copyProperties(schema, copiedRecord);
          return copiedRecord;
        } else {
          return null;
        }
      }
      case ARRAY: {
        Schema itemSchema = schema.getElementType();
        SchemaEntry itemSchemaEntry = new SchemaEntry(schemaEntry.fieldName, itemSchema);
        Schema copiedItemSchema = dropRecursive(itemSchemaEntry, parents, fieldsWithRecursion);
        if (copiedItemSchema == null) {
          return null;
        } else {
          Schema copiedArraySchema = Schema.createArray(copiedItemSchema);
          copyProperties(schema, copiedArraySchema);
          return copiedArraySchema;
        }
      }
      case MAP: {
        Schema valueSchema = schema.getValueType();
        SchemaEntry valueSchemaEntry = new SchemaEntry(schemaEntry.fieldName, valueSchema);
        Schema copiedValueSchema = dropRecursive(valueSchemaEntry, parents, fieldsWithRecursion);
        if (copiedValueSchema == null) {
          return null;
        } else {
          Schema copiedMapSchema = Schema.createMap(copiedValueSchema);
          copyProperties(schema, copiedMapSchema);
          return copiedMapSchema;
        }
      }
      default: {
        return schema;
      }
    }
  }

  /**
   * Annoyingly, Avro doesn't provide a field constructor where you can pass in "unknown to Avro" properties
   * to attach to the field object in the schema even though the Schema language and object model supports it.
   * This method allows for such copiers to explicitly copy the properties from a source field to a destination field.
   * @param sourceField
   * @param copiedField
   */
  private static void copyFieldProperties(Schema.Field sourceField, Schema.Field copiedField) {
    sourceField.getProps().forEach((key, value) -> copiedField.addProp(key, value));
  }

}
