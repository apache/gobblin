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

package gobblin.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
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
import com.google.common.collect.Sets;
import com.google.common.io.Closer;


/**
 * A Utils class for dealing with Avro objects
 */
public class AvroUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  public static final String FIELD_LOCATION_DELIMITER = ".";

  private static final String AVRO_SUFFIX = ".avro";

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
      default:
        throw new AvroRuntimeException("Invalid type in schema : " + schema);
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
        val = ((Record)data).get(pathList.get(field));
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

    AvroUtils.getFieldHelper(retVal, ((Record) data).get(pathList.get(field)), pathList, ++field);
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
    return map.get(utf8Key);
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
          String.format("Cannot convert avro record to new schema. Origianl schema = %s, new schema = %s",
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

    try (InputStream in = fs.open(filePath)) {
      return new Schema.Parser().parse(in);
    }
  }

  public static void writeSchemaToFile(Schema schema, Path filePath, FileSystem fs, boolean overwrite)
      throws IOException {
    writeSchemaToFile(schema, filePath, fs, overwrite, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ));
  }

  public static void writeSchemaToFile(Schema schema, Path filePath, FileSystem fs, boolean overwrite, FsPermission perm)
    throws IOException {
    if (!overwrite) {
      Preconditions.checkState(!fs.exists(filePath), filePath + " already exists");
    } else {
      HadoopUtils.deletePath(fs, filePath, true);
    }

    try (DataOutputStream dos = fs.create(filePath)) {
      dos.writeChars(schema.toString());
    }
    fs.setPermission(filePath, perm);
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
    return removeUncomparableFields(schema, Sets.<Schema> newHashSet());
  }

  private static Optional<Schema> removeUncomparableFields(Schema schema, Set<Schema> processed) {
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

  private static Optional<Schema> removeUncomparableFieldsFromRecord(Schema record, Set<Schema> processed) {
    Preconditions.checkArgument(record.getType() == Schema.Type.RECORD);

    if (processed.contains(record)) {
      return Optional.absent();
    }
    processed.add(record);

    List<Field> fields = Lists.newArrayList();
    for (Field field : record.getFields()) {
      Optional<Schema> newFieldSchema = removeUncomparableFields(field.schema(), processed);
      if (newFieldSchema.isPresent()) {
        fields.add(new Field(field.name(), newFieldSchema.get(), field.doc(), field.defaultValue()));
      }
    }

    Schema newSchema = Schema.createRecord(record.getName(), record.getDoc(), record.getNamespace(), false);
    newSchema.setFields(fields);
    return Optional.of(newSchema);
  }

  private static Optional<Schema> removeUncomparableFieldsFromUnion(Schema union, Set<Schema> processed) {
    Preconditions.checkArgument(union.getType() == Schema.Type.UNION);

    if (processed.contains(union)) {
      return Optional.absent();
    }
    processed.add(union);

    List<Schema> newUnion = Lists.newArrayList();
    for (Schema unionType : union.getTypes()) {
      Optional<Schema> newType = removeUncomparableFields(unionType, processed);
      if (newType.isPresent()) {
        newUnion.add(newType.get());
      }
    }

    // Discard the union field if one or more types are removed from the union.
    if (newUnion.size() != union.getTypes().size()) {
      return Optional.absent();
    }
    return Optional.of(Schema.createUnion(newUnion));
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
   * Deserialize a {@link GenericRecord} from a byte array. This method is not intended for high performance.
   */
  public static GenericRecord slowDeserializeGenericRecord(byte[] serializedRecord, Schema schema) throws IOException {
    Decoder decoder = DecoderFactory.get().binaryDecoder(serializedRecord, null);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }
}
