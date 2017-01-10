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

package gobblin.test;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of {@link Source} for integration test.
 *
 * @author Yinan Li
 */
public class TestSource extends AbstractSource<String, String> {

  static final String SOURCE_FILE_LIST_KEY = "source.files";
  static final String SOURCE_FILE_KEY = "source.file";

  private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    String nameSpace = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    Extract extract1 = createExtract(TableType.SNAPSHOT_ONLY, nameSpace, "TestTable1");
    Extract extract2 = createExtract(TableType.SNAPSHOT_ONLY, nameSpace, "TestTable2");

    String sourceFileList = state.getProp(SOURCE_FILE_LIST_KEY);
    List<String> list = SPLITTER.splitToList(sourceFileList);
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (int i = 0; i < list.size(); i++) {
      WorkUnit workUnit = WorkUnit.create(i % 2 == 0 ? extract1 : extract2);
      workUnit.setProp(SOURCE_FILE_KEY, list.get(i));
      workUnits.add(workUnit);
    }

    if (state.getPropAsBoolean("use.multiworkunit", false)) {
      MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
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
