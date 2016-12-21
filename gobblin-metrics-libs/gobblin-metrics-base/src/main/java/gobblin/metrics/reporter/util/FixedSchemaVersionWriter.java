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

package gobblin.metrics.reporter.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;

import gobblin.annotation.Alias;


/**
 * Write a fixed integer ({@link #SCHEMA_VERSION}) as schema version into the {@link java.io.DataOutputStream}.
 */
@Alias(value = "FIXED_VERSION")
public class FixedSchemaVersionWriter implements SchemaVersionWriter<Integer> {

  public static final int SCHEMA_VERSION = 1;

  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream) throws IOException {
    outputStream.writeInt(SCHEMA_VERSION);
  }

  @Override
  public Integer readSchemaVersioningInformation(DataInputStream inputStream)
      throws IOException {
    return inputStream.readInt();
  }
}
