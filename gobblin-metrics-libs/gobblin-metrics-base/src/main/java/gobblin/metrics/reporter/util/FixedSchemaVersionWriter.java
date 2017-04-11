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
