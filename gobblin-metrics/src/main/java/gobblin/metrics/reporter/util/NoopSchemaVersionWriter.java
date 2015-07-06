/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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


/**
 * Implementation of {@link gobblin.metrics.reporter.util.SchemaVersionWriter} that does not write anything to
 * {@link java.io.DataOutputStream}.
 */
public class NoopSchemaVersionWriter implements SchemaVersionWriter {
  @Override
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream)
      throws IOException {
  }

  @Override
  public Object readSchemaVersioningInformation(DataInputStream inputStream)
      throws IOException {
    return null;
  }
}
