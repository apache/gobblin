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
package gobblin.source.extractor.extract;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.source.extractor.extract.QueryBasedSource.SourceEntity;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.dataset.DatasetUtils;

/**
 * Unit tests for {@link QueryBasedSource}
 */
public class QueryBasedSourceTest {

  @Test
  public void testSourceEntity() {
    SourceEntity se1 = SourceEntity.fromSourceEntityName("SourceEntity1");
    Assert.assertEquals(se1.getSourceEntityName(), "SourceEntity1");
    Assert.assertEquals(se1.getDestTableName(), "SourceEntity1");
    Assert.assertEquals(se1.getDatasetName(), "SourceEntity1");

    SourceEntity se2 = SourceEntity.fromSourceEntityName("SourceEntity$2");
    Assert.assertEquals(se2.getSourceEntityName(), "SourceEntity$2");
    Assert.assertEquals(se2.getDestTableName(), "SourceEntity_2");
    Assert.assertEquals(se2.getDatasetName(), "SourceEntity$2");

    State st1 = new State();
    st1.setProp(ConfigurationKeys.SOURCE_ENTITY, "SourceEntity3");
    st1.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "SourceEntity3_Table");
    Optional<SourceEntity> se3 = SourceEntity.fromState(st1);
    Assert.assertTrue(se3.isPresent());
    Assert.assertEquals(se3.get().getSourceEntityName(), "SourceEntity3");
    Assert.assertEquals(se3.get().getDestTableName(), "SourceEntity3_Table");
    Assert.assertEquals(se3.get().getDatasetName(), "SourceEntity3");
    Assert.assertEquals(se3.get(), new SourceEntity("SourceEntity3", "SourceEntity3_Table"));

    State st2 = new State();
    st2.setProp(ConfigurationKeys.SOURCE_ENTITY, "SourceEntity$4");
    Optional<SourceEntity> se4 = SourceEntity.fromState(st2);
    Assert.assertTrue(se4.isPresent());
    Assert.assertEquals(se4.get(), SourceEntity.fromSourceEntityName("SourceEntity$4"));

    State st3 = new State();
    st3.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "Table5");
    Optional<SourceEntity> se5 = SourceEntity.fromState(st3);
    Assert.assertTrue(se5.isPresent());
    Assert.assertEquals(se5.get(), SourceEntity.fromSourceEntityName("Table5"));
  }

  private Set<SourceEntity> getFilteredEntities(SourceState state) {
    Set<SourceEntity> unfiltered = QueryBasedSource.getSourceEntitiesHelper(state);
    return QueryBasedSource.getFilteredSourceEntitiesHelper(state, unfiltered);
  }

  @Test
  public void testGetFilteredSourceEntities() {
    {
      SourceState state = new SourceState();
      state.setProp(QueryBasedSource.ENTITY_BLACKLIST, "Table1,BadTable.*");
      state.setProp(ConfigurationKeys.SOURCE_ENTITIES, "Table1,Table2,BadTable1,Table3");
      state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "PropShouldBeIgnored");

      Set<SourceEntity> res = getFilteredEntities(state);
      Assert.assertEquals(res.size(), 2);
      Assert.assertTrue(res.contains(SourceEntity.fromSourceEntityName("Table2")),
                       "Missing Table2 in " + res);
      Assert.assertTrue(res.contains(SourceEntity.fromSourceEntityName("Table3")),
          "Missing Table3 in " + res);
    }

    {
      SourceState state = new SourceState();
      state.setProp(QueryBasedSource.ENTITY_BLACKLIST, "Table1,BadTable.*");
      state.setProp(QueryBasedSource.ENTITY_WHITELIST, "Table3");
      state.setProp(ConfigurationKeys.SOURCE_ENTITIES, "Table1,Table2,BadTable1,Table3");
      state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "PropShouldBeIgnored");

      Set<SourceEntity> res = getFilteredEntities(state);
      Assert.assertEquals(res.size(), 1);
      Assert.assertTrue(res.contains(SourceEntity.fromSourceEntityName("Table3")),
          "Missing Table3 in " + res);
    }

    {
      SourceState state = new SourceState();
      state.setProp(QueryBasedSource.ENTITY_BLACKLIST, "Table1,BadTable.*");
      state.setProp(QueryBasedSource.ENTITY_WHITELIST, "Table3");
      state.setProp(ConfigurationKeys.SOURCE_ENTITY, "Table3");
      state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "PropShouldNotBeIgnored");

      Set<SourceEntity> res = getFilteredEntities(state);
      SourceEntity expected = new SourceEntity("Table3", "PropShouldNotBeIgnored");
      Assert.assertEquals(res.size(), 1);
      Assert.assertTrue(res.contains(expected), "Missing Table3 in " + res);
    }

    {
      SourceState state = new SourceState();
      state.setProp(QueryBasedSource.ENTITY_BLACKLIST, "Table1,BadTable.*");
      state.setProp(QueryBasedSource.ENTITY_WHITELIST, "Table5");
      state.setProp(ConfigurationKeys.SOURCE_ENTITY, "Table3");
      state.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "PropShouldNotBeIgnored");

      Set<SourceEntity> res = getFilteredEntities(state);
      Assert.assertEquals(res.size(), 0);
    }
  }

  @Test
  public void testGetTableSpecificPropsFromState() {
    SourceState state = new SourceState();
    state.setProp(DatasetUtils.DATASET_SPECIFIC_PROPS,
        "[{\"dataset\":\"Entity1\", \"value\": 1}, {\"dataset\":\"Table2\", \"value\":2}]");
    // We should look in the dataset specific properties using the entity name, not table name
    SourceEntity se1 = new SourceEntity("Entity1", "Table2");
    SourceEntity se3 = new SourceEntity("Entity3", "Table3");
    Set<SourceEntity> entities = ImmutableSet.of(se1, se3);
    Map<SourceEntity, State> datasetProps =
        QueryBasedSource.getTableSpecificPropsFromState(entities, state);
    // Value 1 should be returned for se1, no prpos should be returned for se3
    Assert.assertEquals(datasetProps.size(), 1);
    Assert.assertTrue(datasetProps.containsKey(se1));
    State se1Props = datasetProps.get(se1);
    Assert.assertEquals(se1Props.getProp("value"), "1");
  }

  @Test
  public void testGetPreviousWatermarksForAllTables() {
    {
      State prevJobState = new SourceState();
      prevJobState.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "full");

      Extract[] extracts = new Extract[3];
      SourceEntity[] sourceEntities = new SourceEntity[extracts.length];
      List<WorkUnitState> prevWuStates = new ArrayList<>();
      // Simulate previous execution with 3 tables and 9 workunits
      // All work units for the Table1 failed.
      // Workunit 0 for Table0 returned no results
      for (int i = 0; i < extracts.length; ++i) {
        String sourceEntityName = "Table$" + i;
        SourceEntity sourceEntity = SourceEntity.fromSourceEntityName(sourceEntityName);
        sourceEntities[i] = sourceEntity;
        extracts[i] = new Extract(TableType.APPEND_ONLY, "", sourceEntity.getDestTableName());
        for (int j = 0; j < 3; ++j) {
          WorkUnit wu = new WorkUnit(extracts[i]);
          wu.setProp(ConfigurationKeys.SOURCE_ENTITY, sourceEntity.getSourceEntityName());
          wu.setProp(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY, 10 * i);
          wu.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, sourceEntity.getDestTableName());
          WorkUnitState wuState = new WorkUnitState(wu, prevJobState);
          wuState.setProp(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK, 20 * i);
          wuState.setProp(ConfigurationKeys.WORK_UNIT_WORKING_STATE_KEY,
              i == 1 ? WorkingState.FAILED.toString() : WorkingState.SUCCESSFUL.toString() );
          wuState.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, (i + j) * 5);

          prevWuStates.add(wuState);
        }
      }

      SourceState prevState = new SourceState(prevJobState, prevWuStates);
      Map<SourceEntity, Long> previousWM =
          QueryBasedSource.getPreviousWatermarksForAllTables(prevState);
      Assert.assertEquals(previousWM.size(), 3);
      // No records read for one WU for Table0: min of all LWM
      Assert.assertEquals(previousWM.get(sourceEntities[0]), Long.valueOf(0L));
      // Failure for Table 1: min of all LWM
      Assert.assertEquals(previousWM.get(sourceEntities[1]), Long.valueOf(10L));
      // Success for Table 2: max of all HWM
      Assert.assertEquals(previousWM.get(sourceEntities[2]), Long.valueOf(40L));

    }

  }

}
