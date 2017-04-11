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


/**
 * Writes schema information to an {@link java.io.DataOutputStream}.
 *
 * <p>
 *   This class is used for prefixing serialized Avro {@link org.apache.avro.generic.GenericData.Record} with
 *   schema information that may help downstream services to parse the record.
 * </p>
 *
 * @param <S> the type of the schema version that can be recovered from {@link java.io.InputStream}s,
 *           i.e. the return type of {@link #readSchemaVersioningInformation(DataInputStream)}
 */
public interface SchemaVersionWriter<S> {

  /**
   * Write schema information to {@link java.io.DataOutputStream}.
   * @param schema Avro {@link org.apache.avro.Schema} of the data that will be serialized into outputStream.
   * @param outputStream {@link java.io.DataOutputStream} where record will be serialized.
   * @throws IOException
   */
  public void writeSchemaVersioningInformation(Schema schema, DataOutputStream outputStream) throws IOException;

  /**
   * Parse schema information from {@link java.io.DataInputStream}, and advance inputStream to the location
   * where actual record starts.
   * @param inputStream {@link java.io.DataInputStream} containing schema information and serialized record.
   * @return Schema information.
   * @throws IOException
   */
  public S readSchemaVersioningInformation(DataInputStream inputStream) throws IOException;

}
