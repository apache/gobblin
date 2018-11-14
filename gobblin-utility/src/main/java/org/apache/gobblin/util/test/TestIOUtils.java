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

package org.apache.gobblin.util.test;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;


/**
 * Test utils to read from and write records from a file
 */
public class TestIOUtils {
  /**
   * Reads all records from a json file as {@link GenericRecord}s
   */
  public static List<GenericRecord> readAllRecords(String jsonDataPath, String schemaPath)
      throws Exception {
    List<GenericRecord> records = new ArrayList<>();
    File jsonDataFile = new File(jsonDataPath);
    File schemaFile = new File(schemaPath);

    Schema schema = new Schema.Parser().parse(schemaFile);
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

    try (InputStream is = new FileInputStream(jsonDataFile)) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, is);
      while (true) {
        records.add(datumReader.read(null, decoder));
      }
    } catch (EOFException eof) {
      // read all records
    }

    return records;
  }
}
