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
package org.apache.gobblin.hive.orc;

import com.google.common.base.Strings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.gobblin.util.orc.AvroOrcSchemaConverter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.TypeDescriptionToObjectInspectorUtil;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveSerDeManager;
import org.apache.gobblin.hive.HiveSerDeWrapper;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.orc.TypeDescription;

/**
 * A derived class of {@link org.apache.gobblin.hive.HiveSerDeManager} that is mainly responsible for adding schema
 * information into {@link HiveRegistrationUnit#serDeProps}, based on the format of the data.
 */
@Slf4j
public class HiveOrcSerDeManager extends HiveSerDeManager {

  // Extensions of files containing ORC data
  public static final String FILE_EXTENSIONS_KEY = "hiveOrcSerdeManager.fileExtensions";
  public static final String DEFAULT_FILE_EXTENSIONS = ".orc";

  // Files with these prefixes are ignored when finding the latest schema
  public static final String IGNORED_FILE_PREFIXES_KEY = "hiveOrcSerdeManager.ignoredPrefixes";
  public static final String DEFAULT_IGNORED_FILE_PREFIXES = "_,.";

  // The serde type
  public static final String SERDE_TYPE_KEY = "hiveOrcSerdeManager.serdeType";
  public static final String DEFAULT_SERDE_TYPE = "ORC";
  public static final String INPUT_FORMAT_CLASS_KEY = "hiveOrcSerdeManager.inputFormatClass";
  public static final String DEFAULT_INPUT_FORMAT_CLASS = OrcInputFormat.class.getName();

  public static final String OUTPUT_FORMAT_CLASS_KEY = "hiveOrcSerdeManager.outputFormatClass";
  public static final String DEFAULT_OUTPUT_FORMAT_CLASS = OrcOutputFormat.class.getName();

  public static final String HIVE_SPEC_SCHEMA_READING_TIMER = "hiveOrcSerdeManager.schemaReadTimer";

  public static final String ENABLED_ORC_TYPE_CHECK = "hiveOrcSerdeManager.enableFormatCheck";
  public static final boolean DEFAULT_ENABLED_ORC_TYPE_CHECK = false;

  private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;
  private static final String ORC_FORMAT = "ORC";
  private static final ByteBuffer MAGIC_BUFFER = ByteBuffer.wrap(ORC_FORMAT.getBytes(Charsets.UTF_8));

  private final FileSystem fs;
  private final HiveSerDeWrapper serDeWrapper;
  private final List<String> fileExtensions;
  private final List<String> ignoredFilePrefixes;
  private final boolean checkOrcFormat;
  private final MetricContext metricContext;

  public HiveOrcSerDeManager(State props)
      throws IOException {
    super(props);
    this.fs = FileSystem.get(HadoopUtils.getConfFromState(props));

    List<String> extensions = props.getPropAsList(FILE_EXTENSIONS_KEY, DEFAULT_FILE_EXTENSIONS);
    this.fileExtensions = extensions.isEmpty() ? ImmutableList.of("") : extensions;

    this.ignoredFilePrefixes = props.getPropAsList(IGNORED_FILE_PREFIXES_KEY, DEFAULT_IGNORED_FILE_PREFIXES);
    this.checkOrcFormat = props.getPropAsBoolean(ENABLED_ORC_TYPE_CHECK, DEFAULT_ENABLED_ORC_TYPE_CHECK);
    this.metricContext = Instrumented.getMetricContext(props, HiveOrcSerDeManager.class);
    this.serDeWrapper = HiveSerDeWrapper.get(props.getProp(SERDE_TYPE_KEY, DEFAULT_SERDE_TYPE),
        Optional.of(props.getProp(INPUT_FORMAT_CLASS_KEY, DEFAULT_INPUT_FORMAT_CLASS)),
        Optional.of(props.getProp(OUTPUT_FORMAT_CLASS_KEY, DEFAULT_OUTPUT_FORMAT_CLASS)));
  }

  @Override
  //Using LIST_COLUMNS and LIST_COLUMN_TYPES to compare schema
  public boolean haveSameSchema(HiveRegistrationUnit unit1, HiveRegistrationUnit unit2)
      throws IOException {
    if (unit1.getSerDeProps().contains(serdeConstants.LIST_COLUMNS) && unit2.getSerDeProps().contains(serdeConstants.LIST_COLUMNS)
    && unit1.getSerDeProps().contains(serdeConstants.LIST_COLUMN_TYPES) && unit2.getSerDeProps().contains(serdeConstants.LIST_COLUMN_TYPES)) {
      return unit1.getSerDeProps().getProp(serdeConstants.LIST_COLUMNS).equals(unit2.getSerDeProps().getProp(serdeConstants.LIST_COLUMNS))
          && unit1.getSerDeProps().getProp(serdeConstants.LIST_COLUMN_TYPES).equals(unit2.getSerDeProps().getProp(serdeConstants.LIST_COLUMN_TYPES));
    } else {
      return false;
    }
  }

  /**
   * Add ORC SerDe attributes into HiveUnit
   *
   * @param path
   * @param hiveUnit
   * @throws IOException
   */
  @Override
  public void addSerDeProperties(Path path, HiveRegistrationUnit hiveUnit)
      throws IOException {
    hiveUnit.setSerDeType(this.serDeWrapper.getSerDe().getClass().getName());
    hiveUnit.setInputFormat(this.serDeWrapper.getInputFormatClassName());
    hiveUnit.setOutputFormat(this.serDeWrapper.getOutputFormatClassName());

    addSchemaProperties(path, hiveUnit);
  }

  @Override
  public void addSerDeProperties(HiveRegistrationUnit source, HiveRegistrationUnit target)
      throws IOException {
    if (source.getSerDeType().isPresent()) {
      target.setSerDeType(source.getSerDeType().get());
    }
    if (source.getInputFormat().isPresent()) {
      target.setInputFormat(source.getInputFormat().get());
    }
    if (source.getOutputFormat().isPresent()) {
      target.setOutputFormat(source.getOutputFormat().get());
    }
  }

  @Override
  public void updateSchema(HiveRegistrationUnit existingUnit, HiveRegistrationUnit newUnit)
      throws IOException {
    Preconditions.checkArgument(
        newUnit.getSerDeProps().contains(serdeConstants.LIST_COLUMNS));
    Preconditions.checkArgument(
        newUnit.getSerDeProps().contains(serdeConstants.LIST_COLUMN_TYPES));

    existingUnit.setSerDeProp(serdeConstants.LIST_COLUMNS, newUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMNS));
    existingUnit.setSerDeProp(serdeConstants.LIST_COLUMN_TYPES, newUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMN_TYPES));
  }

  /**
   * Get the schema as a TypeInfo object
   * @param path path that contains the ORC files
   * @param fs {@link FileSystem}
   * @return {@link TypeInfo} with the schema information
   * @throws IOException
   */
  public TypeInfo getSchemaFromLatestFile(Path path, FileSystem fs)
      throws IOException {
    if (fs.isDirectory(path)) {
      List<FileStatus> files = Arrays.asList(fs.listStatus(path, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          try {
            return ignoredFilePrefixes.stream().noneMatch(e -> path.getName().startsWith(e))
                && fileExtensions.stream().anyMatch(e -> path.getName().endsWith(e))
                && (!checkOrcFormat || isORC(path, fs));
          } catch(IOException e) {
            log.error("Error checking file for schema retrieval", e);
            return false;
          }
        }
      }));

      if (files.size() > 0) {
        Collections.sort((files), FileListUtils.LATEST_MOD_TIME_ORDER);
      } else {
        throw new FileNotFoundException("No files in Dataset:" + path + " found for schema retrieval");
      }
      return getSchemaFromLatestFile(files.get(0).getPath(), fs);
    } else {
      return TypeInfoUtils.getTypeInfoFromObjectInspector(OrcFile.createReader(fs, path).getObjectInspector());
    }
  }

  /**
   * Determine if a file is ORC format.
   * Steal ideas & code from presto/OrcReader under Apache License 2.0.
   *
   * Note: This operation is pretty expensive when it comes to checking magicBytes for each file while listing,
   * as itself require getFileStatus and open the file.  In normal cases, consider disable it if the confidene level
   * of format consistency is high enough.
   */
  private static boolean isORC(Path file, FileSystem fs)
      throws IOException {
    try {
      FSDataInputStream inputStream = fs.open(file);
      long size = fs.getFileStatus(file).getLen();
      byte[] buffer = new byte[Math.toIntExact(Math.min(size, EXPECTED_FOOTER_SIZE))];
      if (size < buffer.length) {
        return false;
      }

      inputStream.readFully(size - buffer.length, buffer);

      // get length of PostScript - last byte of the file
      int postScriptSize = buffer[buffer.length - 1] & 0xff;
      int magicLen = MAGIC_BUFFER.remaining();

      if (postScriptSize < magicLen + 1 || postScriptSize >= buffer.length) {
        return false;
      }

      if (!MAGIC_BUFFER.equals(ByteBuffer.wrap(buffer, buffer.length - 1 - magicLen, magicLen))) {
        // Old versions of ORC (0.11) wrote the magic to the head of the file
        byte[] headerMagic = new byte[magicLen];
        inputStream.readFully(0, headerMagic);

        // if it isn't there, this isn't an ORC file
        if (!MAGIC_BUFFER.equals(ByteBuffer.wrap(headerMagic))) {
          return false;
        }
      }

      return true;
    } catch (Exception e) {
      throw new RuntimeException("Error occured when checking the type of file:" + file);
    }
  }

  private void addSchemaProperties(Path path, HiveRegistrationUnit hiveUnit)
      throws IOException {
    Preconditions.checkArgument(this.fs.getFileStatus(path).isDirectory(), path + " is not a directory.");
    try (Timer.Context context = metricContext.timer(HIVE_SPEC_SCHEMA_READING_TIMER).time()) {
      addSchemaPropertiesHelper(path, hiveUnit);
    }
  }

  /**
   * Extensible if there's other source-of-truth for fetching schema instead of interacting with HDFS.
   *
   * For purpose of initializing {@link org.apache.hadoop.hive.ql.io.orc.OrcSerde} object, it will require:
   * org.apache.hadoop.hive.serde.serdeConstants#LIST_COLUMNS and
   * org.apache.hadoop.hive.serde.serdeConstants#LIST_COLUMN_TYPES
   *
   */
  protected void addSchemaPropertiesHelper(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
    TypeInfo schema;
    String schemaString = hiveUnit.getSerDeProps()
        .getProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
            props.getProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()));
    if (!Strings.isNullOrEmpty(schemaString)) {
      Schema avroSchema =
          new Schema.Parser().parse(schemaString);
      TypeDescription orcSchema = AvroOrcSchemaConverter.getOrcSchema(avroSchema);
      schema = TypeInfoUtils.getTypeInfoFromObjectInspector(
          TypeDescriptionToObjectInspectorUtil.getObjectInspector(orcSchema));
    } else {
      schema = getSchemaFromLatestFile(path, this.fs);
    }
    if (schema instanceof StructTypeInfo) {
      StructTypeInfo structTypeInfo = (StructTypeInfo) schema;
      hiveUnit.setSerDeProp(serdeConstants.LIST_COLUMNS, Joiner.on(",").join(structTypeInfo.getAllStructFieldNames()));
      hiveUnit.setSerDeProp(serdeConstants.LIST_COLUMN_TYPES, Joiner.on(",")
          .join(structTypeInfo.getAllStructFieldTypeInfos()
              .stream()
              .map(x -> x.getTypeName())
              .collect(Collectors.toList())));
    } else {
      // Hive always uses a struct with a field for each of the top-level columns as the root object type.
      // So for here we assume to-be-registered ORC files follow this pattern.
      throw new IllegalStateException("A valid ORC schema should be an instance of struct");
    }
  }
}
