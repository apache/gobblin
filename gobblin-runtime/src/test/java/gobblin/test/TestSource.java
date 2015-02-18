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

package gobblin.test;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link Source} for integration test.
 *
 * @author ynli
 */
public class TestSource implements Source<String, String> {

  private static final String SOURCE_FILE_LIST_KEY = "source.files";
  private static final String SOURCE_FILE_KEY = "source.file";

  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    Extract extract1 = state
        .createExtract(TableType.SNAPSHOT_ONLY, state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY),
            "TestTable1");

    Extract extract2 = state
        .createExtract(TableType.SNAPSHOT_ONLY, state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY),
            "TestTable2");

    String sourceFileList = state.getProp(SOURCE_FILE_LIST_KEY);
    List<String> list = SPLITTER.splitToList(sourceFileList);
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (int i = 0; i < list.size(); i++) {
      WorkUnit workUnit = new WorkUnit(state, i % 2 == 0 ? extract1 : extract2);
      workUnit.setProp(SOURCE_FILE_KEY, list.get(i));
      workUnits.add(workUnit);
    }

    if (state.getPropAsBoolean("use.multiworkunit", false)) {
      MultiWorkUnit multiWorkUnit = new MultiWorkUnit();
      multiWorkUnit.addWorkUnits(workUnits);
      workUnits.clear();
      workUnits.add(multiWorkUnit);
    }

    return workUnits;
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) {
    return new TestExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // Do nothing
  }
}
