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

package org.apache.gobblin.data.management.copy.extractor;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.FileAwareInputStream;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.util.HadoopUtils;



public class FileAwareInputStreamExtractorWithCheckSchema extends FileAwareInputStreamExtractor{
  public final static String EXPECTED_SCHEMA = "expected.schema";

  public FileAwareInputStreamExtractorWithCheckSchema(FileSystem fs, CopyableFile file, WorkUnitState state)
  {
    super(fs, file, state);
  }
  public FileAwareInputStreamExtractorWithCheckSchema(FileSystem fs, CopyableFile file)
  {
    this(fs, file, null);
  }

  @Override
  public FileAwareInputStream readRecord(@Deprecated FileAwareInputStream reuse)
      throws DataRecordException, IOException {
    if (!this.recordRead) {
      Configuration conf =
          this.state == null ? HadoopUtils.newConfiguration() : HadoopUtils.getConfFromState(this.state);
      FileSystem fsFromFile = this.file.getOrigin().getPath().getFileSystem(conf);
      if(!schemaChecking(fsFromFile))
      {
        throw new DataRecordException("Schema does match the expected schema");
      }
      return this.buildStream(fsFromFile);
    }
    return null;
  }

  protected boolean schemaChecking(FileSystem fsFromFile)
      throws IOException {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader(new FsInput(this.file.getFileStatus().getPath(),fsFromFile), datumReader);
    Schema schema = dataFileReader.getSchema();
    return schema.toString().equals(this.state.getProp(EXPECTED_SCHEMA));
  }
}
