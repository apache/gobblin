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
package org.apache.gobblin.data.management.conversion.hive.watermarker;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.Watermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * A {@link HiveSourceWatermarker} that manages {@link Watermark} at a per {@link Table} basis.
 * <ul>
 * <li>One {@link Watermark} per table exists.
 * <li>All {@link Partition}s of a {@link Table} have the same {@link Watermark}.
 * <li>The time at which the job processed a {@link Table} for workunit creation is used as {@link Watermark}
 * </ul>
 *
 */
@Slf4j
public class TableLevelWatermarker implements HiveSourceWatermarker {
  public static final Gson GSON = new Gson();

  // Table complete name db@tb - list of previous workunitState
  protected Map<String, LongWatermark> tableWatermarks;

  public TableLevelWatermarker(State state) {
    this.tableWatermarks = Maps.newHashMap();

    // Load previous watermarks in case of sourceState
    if (state instanceof SourceState) {
      SourceState sourceState = (SourceState)state;
      for (Map.Entry<String, Iterable<WorkUnitState>> datasetWorkUnitStates : sourceState
          .getPreviousWorkUnitStatesByDatasetUrns().entrySet()) {

        // Use the minimum of all previous watermarks for this dataset
        List<LongWatermark> previousWatermarks = FluentIterable.from(datasetWorkUnitStates.getValue())
        .filter(Predicates.not(PartitionLevelWatermarker.WATERMARK_WORKUNIT_PREDICATE))
        .transform(new Function<WorkUnitState, LongWatermark>() {
          @Override
          public LongWatermark apply(WorkUnitState w) {
            return w.getActualHighWatermark(LongWatermark.class);
          }
        }).toList();

        if (!previousWatermarks.isEmpty()) {
          this.tableWatermarks.put(datasetWorkUnitStates.getKey(), Collections.min(previousWatermarks));
        }
      }
      log.debug("Loaded table watermarks from previous state " + this.tableWatermarks);
    }
  }

  @Override
  public LongWatermark getPreviousHighWatermark(Table table) {
    if (this.tableWatermarks.containsKey(table.getCompleteName())) {
      return this.tableWatermarks.get(table.getCompleteName());
    }
    return new LongWatermark(0);
  }

  @Override
  public LongWatermark getPreviousHighWatermark(Partition partition) {
    return getPreviousHighWatermark(partition.getTable());
  }

  @Override
  public void onTableProcessBegin(Table table, long tableProcessTime) {}

  @Override
  public void onPartitionProcessBegin(Partition partition, long partitionProcessTime, long partitionUpdateTime) {}

  @Override
  public void onGetWorkunitsEnd(List<WorkUnit> workunits) {}

  @Override
  public LongWatermark getExpectedHighWatermark(Table table, long tableProcessTime) {
    return new LongWatermark(tableProcessTime);
  }

  @Override
  public LongWatermark getExpectedHighWatermark(Partition partition, long tableProcessTime, long partitionProcessTime) {
    return getExpectedHighWatermark(partition.getTable(), tableProcessTime);
  }

  @Override
  public void setActualHighWatermark(WorkUnitState wus) {
    wus.setActualHighWatermark(wus.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
  }

  /**
   * Factory to create a {@link TableLevelWatermarker}
   */
  public static class Factory implements HiveSourceWatermarkerFactory {

    @Override
    public TableLevelWatermarker createFromState(State state) {
      return new TableLevelWatermarker(state);
    }
  }
}
