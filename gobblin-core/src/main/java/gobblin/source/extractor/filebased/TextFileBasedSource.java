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

package gobblin.source.extractor.filebased;

import java.io.IOException;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.hadoop.HadoopFsHelper;

import static gobblin.configuration.ConfigurationKeys.*;


/**
 * A source that reads text based input from a directory as lines.
 */
public class TextFileBasedSource extends FileBasedSource<String, String> {

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
    if (!state.contains(SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS)) {
      state.setProp(SOURCE_FILEBASED_OPTIONAL_DOWNLOADER_CLASS, TokenizedFileDownloader.class.getName());
    }
    return new FileBasedExtractor<>(state, new HadoopFsHelper(state));
  }

  @Override
  public void initFileSystemHelper(State state) throws FileBasedHelperException {
    this.fsHelper = new HadoopFsHelper(state);
    this.fsHelper.connect();
  }

  @Override
  protected String getLsPattern(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY);
  }
}
