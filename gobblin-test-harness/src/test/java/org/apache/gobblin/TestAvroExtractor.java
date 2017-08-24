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
package org.apache.gobblin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;


public class TestAvroExtractor implements Extractor<Schema, GenericRecord> {
  private WorkUnitState state;
  private List<GenericRecord> recordList;
  private Iterator<GenericRecord> recordIterator;

  public TestAvroExtractor(WorkUnitState workUnitState)
      throws IOException {
    this.state = workUnitState;
    this.recordList =getRecordFromFile(workUnitState.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
    this.recordIterator = this.recordList.iterator();
  }

  public static List<GenericRecord> getRecordFromFile(String path)
      throws IOException {
    Configuration config = new Configuration();
    SeekableInput input = new FsInput(new Path(path), config);
    DatumReader<GenericRecord> reader1 = new GenericDatumReader<>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader1);
    List<GenericRecord> records = new ArrayList<>();
    for (GenericRecord datum : fileReader) {
      records.add(datum);
    }
    fileReader.close();
    return records;
  }

  @Override
  public Schema getSchema()
      throws IOException {
    if (recordList == null) {
      return null;
    }
    if (recordList.isEmpty()) {
      return null;
    }
    return recordList.get(0).getSchema();
  }

  @Override
  public GenericRecord readRecord(@Deprecated GenericRecord reuse)
      throws DataRecordException, IOException {
    if (this.recordIterator.hasNext()) {
      return this.recordIterator.next();
    } else {
      return null;
    }
  }

  @Override
  public long getExpectedRecordCount() {
    return recordList.size();
  }

  @Override
  public long getHighWatermark() {
    return recordList.size();
  }

  @Override
  public void close()
      throws IOException {
  }
}
