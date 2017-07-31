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

package org.apache.gobblin.example.simplejson;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link Source} for the simple JSON example.
 *
 * <p>
 *   This source creates one {@link org.apache.gobblin.source.workunit.WorkUnit}
 *   for each file to pull and uses the {@link SimpleJsonExtractor} to pull the data.
 * </p>
 *
 * @author Yinan Li
 */
public class SimpleJsonSource implements Source<String, String> {

  public static final String SOURCE_FILE_KEY = "source.file";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    if (!state.contains(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL)) {
      return workUnits;
    }

    // Create a single snapshot-type extract for all files
    Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY,
        state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "ExampleNamespace"), "ExampleTable");

    String filesToPull = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL);
    for (String file : Splitter.on(',').omitEmptyStrings().split(filesToPull)) {
      // Create one work unit for each file to pull
      WorkUnit workUnit = WorkUnit.create(extract);
      workUnit.setProp(SOURCE_FILE_KEY, file);
      workUnits.add(workUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
    return new SimpleJsonExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }
}
