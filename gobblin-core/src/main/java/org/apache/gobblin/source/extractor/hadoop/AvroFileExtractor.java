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

package org.apache.gobblin.source.extractor.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Throwables;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.filebased.FileBasedExtractor;
import org.apache.gobblin.source.extractor.filebased.FileBasedHelperException;


/**
 * A custom type of {@link FileBasedExtractor}s for extracting data from Avro files.
 */
public class AvroFileExtractor extends FileBasedExtractor<Schema, GenericRecord> {

  public AvroFileExtractor(WorkUnitState workUnitState) {
    super(workUnitState, new AvroFsHelper(workUnitState));
  }

  @Override
  public Iterator<GenericRecord> downloadFile(String file) throws IOException {
    try {
      return this.closer.register(((AvroFsHelper) this.fsHelper).getAvroFile(file));
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }
    return null;
  }

  /**
   * Assumption is that all files in the input directory have the same schema
   */
  @Override
  public Schema getSchema() {
    if (this.workUnit.contains(ConfigurationKeys.SOURCE_SCHEMA)) {
      return new Schema.Parser().parse(this.workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA));
    }

    AvroFsHelper hfsHelper = (AvroFsHelper) this.fsHelper;
    if (this.filesToPull.isEmpty()) {
      return null;
    }
    try {
      return hfsHelper.getAvroSchema(this.filesToPull.get(0));
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
      return null;
    }
  }
}
