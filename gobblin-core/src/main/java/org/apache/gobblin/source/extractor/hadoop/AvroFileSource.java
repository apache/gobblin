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

package gobblin.source.extractor.hadoop;

import gobblin.source.extractor.filebased.FileBasedHelperException;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedSource;


public class AvroFileSource extends FileBasedSource<Schema, GenericRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroFileSource.class);

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state) throws IOException {
    return new AvroFileExtractor(state);
  }

  @Override
  public void initFileSystemHelper(State state) throws FileBasedHelperException {
    this.fsHelper = new AvroFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = Lists.newArrayList();
    String path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);

    try {
      LOGGER.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
    } catch (FileBasedHelperException e) {
      LOGGER.error("Not able to run ls command due to " + e.getMessage() + " will not pull any files", e);
    }
    return results;
  }
}
