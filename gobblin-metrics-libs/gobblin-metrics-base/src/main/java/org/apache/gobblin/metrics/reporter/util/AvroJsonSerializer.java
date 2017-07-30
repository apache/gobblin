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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;


/**
 * {@link gobblin.metrics.reporter.util.AvroSerializer} that serializes into JSON.
 */
public class AvroJsonSerializer<T extends SpecificRecord> extends AvroSerializer<T> {

  public AvroJsonSerializer(Schema schema, SchemaVersionWriter schemaVersionWriter)
      throws IOException {
    super(schema, schemaVersionWriter);
  }

  @Override
  protected Encoder getEncoder(Schema schema, OutputStream outputStream) throws IOException {
    return EncoderFactory.get().jsonEncoder(schema, outputStream);
  }
}
