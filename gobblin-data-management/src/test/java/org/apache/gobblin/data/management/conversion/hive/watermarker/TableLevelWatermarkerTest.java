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
package gobblin.data.management.conversion.hive.watermarker;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.LongWatermark;


@Test(groups = { "gobblin.data.management.conversion" })
public class TableLevelWatermarkerTest {

  @Test
  public void testPreviousState() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_table");
    previousWus.setActualHighWatermark(new LongWatermark(100l));

    // Watermark will be lowest of 100l and 101l
    WorkUnitState previousWus1 = new WorkUnitState();
    previousWus1.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_table");
    previousWus1.setActualHighWatermark(new LongWatermark(101l));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    TableLevelWatermarker watermarker = new TableLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousHighWatermark(mockTable("test_table")), new LongWatermark(100l));

  }

  @Test
  public void testPreviousStateWithPartitionWatermark() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_table");
    previousWus.setActualHighWatermark(new LongWatermark(100l));

    // Watermark workunits created by PartitionLevelWatermarker need to be ignored.
    WorkUnitState previousWus1 = new WorkUnitState();
    previousWus1.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_table");
    previousWus1.setProp(PartitionLevelWatermarker.IS_WATERMARK_WORKUNIT_KEY, true);
    previousWus1.setActualHighWatermark(new MultiKeyValueLongWatermark(ImmutableMap.of("part1", 200l)));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    TableLevelWatermarker watermarker = new TableLevelWatermarker(state);

    Assert.assertEquals(watermarker.getPreviousHighWatermark(mockTable("test_table")), new LongWatermark(100l));

  }

  /**
   * Make sure that all partitions get the same previous high watermark (table's watermark)
   */
  @Test
  public void testPartitionWatermarks() throws Exception {
    WorkUnitState previousWus = new WorkUnitState();
    previousWus.setProp(ConfigurationKeys.DATASET_URN_KEY, "test_table");
    previousWus.setActualHighWatermark(new LongWatermark(100l));

    SourceState state = new SourceState(new State(), Lists.newArrayList(previousWus));
    TableLevelWatermarker watermarker = new TableLevelWatermarker(state);

    Table mockTable = mockTable("test_table");
    Assert.assertEquals(watermarker.getPreviousHighWatermark(mockTable), new LongWatermark(100l));
    Assert.assertEquals(watermarker.getPreviousHighWatermark(mockPartition(mockTable, ImmutableList.of("2015"))),
        new LongWatermark(100l));
    Assert.assertEquals(watermarker.getPreviousHighWatermark(mockPartition(mockTable, ImmutableList.of("2016"))),
        new LongWatermark(100l));
  }

  private static Table mockTable(String name) {
    Table table = Mockito.mock(Table.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(table.getCompleteName()).thenReturn(name);
    return table;
  }

  private static Partition mockPartition(Table table, List<String> values) {
    Partition partition = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(partition.getTable()).thenReturn(table);
    Mockito.when(partition.getValues()).thenReturn(values);
    return partition;
  }
}
