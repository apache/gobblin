/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.hadoop;

import gobblin.source.extractor.filebased.FileBasedHelperException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.filebased.FileBasedSource;


public class HadoopSource extends FileBasedSource<Schema, GenericRecord> {
  private Logger log = LoggerFactory.getLogger(HadoopSource.class);

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    return new HadoopExtractor(state);
  }

  @Override
  public void initFileSystemHelper(State state)
      throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results = new ArrayList<String>();
    String path = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);

    try {
      log.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
    } catch (FileBasedHelperException e) {
      log.error("Not able to run ls command due to " + e.getMessage() + " will not pull any files", e);
    }
    return results;
  }
}
