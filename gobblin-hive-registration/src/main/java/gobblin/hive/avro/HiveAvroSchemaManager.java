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
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveSchemaManager;
import gobblin.util.AvroUtils;
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link HiveSchemaManager} for registering Avro tables and partitions.
 *
 * @author ziliu
 */
@Slf4j
@Alpha
public class HiveAvroSchemaManager extends HiveSchemaManager {

  public static final String SCHEMA_LITERAL = "avro.schema.literal";

  private static final String AVRO = "avro";

  private final FileSystem fs;

  public HiveAvroSchemaManager(State props) throws IOException {
    super(props);
    this.fs = FileSystem.get(HadoopUtils.getConfFromState(props));
  }

  @Override
  public void addSchemaProperties(SerDeInfo si, Path path) {
    Schema schema = getSchema(path);

    Preconditions.checkNotNull(schema);
    si.getParameters().put(SCHEMA_LITERAL, schema.toString());
  }

  @SuppressWarnings("deprecation")
  private Schema getSchema(Path path) {
    try {
      for (FileStatus status : this.fs.listStatus(path)) {
        if (status.isDir()) {
          Schema schema = getSchema(status.getPath());
          if (schema != null) {
            return schema;
          }
        } else if (FilenameUtils.isExtension(status.getPath().getName().toLowerCase(), AVRO)) {
          return AvroUtils.getSchemaFromDataFile(status.getPath(), this.fs);
        }
      }
      return null;
    } catch (IOException e) {
      log.error("Unable to get schema from " + path, e);
      throw Throwables.propagate(e);
    }
  }

}
