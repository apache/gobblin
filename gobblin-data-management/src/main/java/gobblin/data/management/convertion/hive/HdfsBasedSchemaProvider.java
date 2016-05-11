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
package gobblin.data.management.convertion.hive;

import java.io.IOException;

import lombok.AllArgsConstructor;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;
import gobblin.util.AvroUtils;


/**
 * A {@link HiveAvroSchemaProvider} that uses the physical avro files of a {@link HiveTable} or a {@link HivePartition}
 * on HDFS to find the {@link Schema}.
 */
@AllArgsConstructor
public class HdfsBasedSchemaProvider implements HiveAvroSchemaProvider {

  private FileSystem fs;

  /**
   * Get the latest avro file {@link Schema} in the directory {@link HiveRegistrationUnit#getLocation()}
   * @throws IOException if no {@link Schema} was found
   *
   * {@inheritDoc}
   * @see gobblin.data.management.convertion.hive.HiveAvroSchemaProvider#getSchema(gobblin.hive.HiveRegistrationUnit)
   */
  @Override
  public Schema getSchema(HiveRegistrationUnit hiveUnit) throws IOException {
    if (hiveUnit.getLocation().isPresent()) {
      Schema avroSchema = AvroUtils.getDirectorySchema(new Path(hiveUnit.getLocation().get()), this.fs, true);
      if (avroSchema == null) {
        throw new IOException(String.format("No avro schema found for directory %s ", hiveUnit.getLocation().get()));
      }
      return avroSchema;
    }

    throw new IOException(String.format("No data location found for hiveUnit %s ", hiveUnit));
  }
}
