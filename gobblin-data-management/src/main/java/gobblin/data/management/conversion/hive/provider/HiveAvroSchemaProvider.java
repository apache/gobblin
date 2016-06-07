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
package gobblin.data.management.conversion.hive.provider;

import java.io.IOException;

import org.apache.avro.Schema;

import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegistrationUnit;
import gobblin.hive.HiveTable;

/**
 * An abstraction to find Avro {@link Schema} or a {@link HiveTable} or a {@link HivePartition}
 */
public interface HiveAvroSchemaProvider {
  /**
   * Get the Avro {@link Schema} for the {@link HiveRegistrationUnit}
   *
   * @param hiveUnit can be a {@link HiveTable} or a {@link HivePartition}
   * @return The Avro {@link Schema} for this {@link HiveTable} or {@link HivePartition}
   */
  public Schema getSchema(HiveRegistrationUnit hiveUnit) throws IOException;
}
