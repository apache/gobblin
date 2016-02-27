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

package gobblin.hive.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveSerDeManager;
import gobblin.hive.HiveSerDeWrapper;
import gobblin.util.AvroUtils;
import gobblin.util.HadoopUtils;


/**
 * A {@link HiveSerDeManager} for registering Avro tables and partitions.
 *
 * @author ziliu
 */
@Alpha
public class HiveAvroSerDeManager extends HiveSerDeManager {

  public static final String SCHEMA_LITERAL = "avro.schema.literal";
  public static final String SCHEMA_URL = "avro.schema.url";
  public static final String USE_SCHEMA_FILE = "use.schema.file";
  public static final boolean DEFAULT_USE_SCHEMA_FILE = false;
  public static final String SCHEMA_FILE_NAME = "schema.file.name";
  public static final String DEFAULT_SCHEMA_FILE_NAME = "_schema.avsc";
  public static final String SCHEMA_LITERAL_LENGTH_LIMIT = "schema.literal.length.limit";
  public static final int DEFAULT_SCHEMA_LITERAL_LENGTH_LIMIT = 4000;

  protected final FileSystem fs;
  protected final boolean useSchemaFile;
  protected final String schemaFileName;
  protected final int schemaLiteralLengthLimit;
  protected final HiveSerDeWrapper serDeWrapper = HiveSerDeWrapper.get("AVRO");

  public HiveAvroSerDeManager(State props) throws IOException {
    super(props);
    this.fs = FileSystem.get(HadoopUtils.getConfFromState(props));
    this.useSchemaFile = props.getPropAsBoolean(USE_SCHEMA_FILE, DEFAULT_USE_SCHEMA_FILE);
    this.schemaFileName = props.getProp(SCHEMA_FILE_NAME, DEFAULT_SCHEMA_FILE_NAME);
    this.schemaLiteralLengthLimit =
        props.getPropAsInt(SCHEMA_LITERAL_LENGTH_LIMIT, DEFAULT_SCHEMA_LITERAL_LENGTH_LIMIT);
  }

  /**
   * Add an Avro {@link Schema} to the given {@link HiveRegistrationUnit}. 
   *
   *  <p>
   *    If {@link #USE_SCHEMA_FILE} is true, the schema will be added via {@link #SCHEMA_URL} pointing to
   *    the schema file named {@link #SCHEMA_FILE_NAME}.
   *  </p>
   *
   *  <p>
   *    If {@link #USE_SCHEMA_FILE} is false, the schema will be obtained by {@link #getDirectorySchema(Path)}. 
   *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via 
   *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
   *    via {@link #SCHEMA_URL}.
   *  </p>
   */
  @Override
  public void addSerDeProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
    hiveUnit.setSerDeType(this.serDeWrapper.getSerDe().getClass().getName());
    hiveUnit.setInputFormat(this.serDeWrapper.getInputFormatClassName());
    hiveUnit.setOutputFormat(this.serDeWrapper.getOutputFormatClassName());

    addSchemaProperties(path, hiveUnit);
  }

  @SuppressWarnings("deprecation")
  private void addSchemaProperties(Path path, HiveRegistrationUnit hiveUnit) throws IOException {
    Preconditions.checkArgument(this.fs.getFileStatus(path).isDir(), path + " is not a directory.");

    Path schemaFile = new Path(path, this.schemaFileName);
    if (this.useSchemaFile) {
      hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
    } else {
      Schema schema = getDirectorySchema(path);
      addSchemaFromAvroFile(schema, schemaFile, hiveUnit);
    }
  }

  /**
   * Get schema for a directory using {@link AvroUtils#getDirectorySchema(Path, FileSystem, boolean)}.
   */
  protected Schema getDirectorySchema(Path directory) throws IOException {
    return AvroUtils.getDirectorySchema(directory, this.fs, true);
  }

  /**
   * Add a {@link Schema} obtained from an Avro data file to the given {@link HiveRegistrationUnit}.
   *
   *  <p>
   *    If the length of the schema is less than {@link #SCHEMA_LITERAL_LENGTH_LIMIT}, it will be added via
   *    {@link #SCHEMA_LITERAL}. Otherwise, the schema will be written to {@link #SCHEMA_FILE_NAME} and added
   *    via {@link #SCHEMA_URL}.
   *  </p>
   */
  protected void addSchemaFromAvroFile(Schema schema, Path schemaFile, HiveRegistrationUnit hiveUnit)
      throws IOException {
    Preconditions.checkNotNull(schema);

    String schemaStr = schema.toString();
    if (schemaStr.length() <= this.schemaLiteralLengthLimit) {
      hiveUnit.setSerDeProp(SCHEMA_LITERAL, schema.toString());
    } else {
      AvroUtils.writeSchemaToFile(schema, schemaFile, this.fs, true);
      hiveUnit.setSerDeProp(SCHEMA_URL, schemaFile.toString());
    }
  }

  @Override
  public void updateSchema(HiveRegistrationUnit existingUnit, HiveRegistrationUnit newUnit) throws IOException {
    Preconditions.checkArgument(
        newUnit.getSerDeProps().contains(SCHEMA_LITERAL) || newUnit.getSerDeProps().contains(SCHEMA_URL));

    if (newUnit.getSerDeProps().contains(SCHEMA_LITERAL)) {
      existingUnit.setSerDeProp(SCHEMA_LITERAL, newUnit.getSerDeProps().getProp(SCHEMA_LITERAL));
    } else {
      existingUnit.setSerDeProp(SCHEMA_URL, newUnit.getSerDeProps().getProp(SCHEMA_URL));
    }
  }

}
