/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.convertion.hive;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.extract.LongWatermark;


/**
 * A {@link HiveSourceWatermarker} that manages {@link Watermark} at a per {@link Table} basis.
 * One {@link Watermark} per table exists.
 * All {@link Partition}s of a {@link Table} have the same {@link Watermark}.
 */
public class TableLevelWatermarker implements HiveSourceWatermarker {
  public static final Gson GSON = new Gson();

  // Table complete name db@tb - list of previous workunitState
  private Map<String, LongWatermark> tableWatermarks;

  public TableLevelWatermarker(SourceState state) {
    this.tableWatermarks = Maps.newHashMap();
    SourceState sourceState = (SourceState) state;

    for (Map.Entry<String, Iterable<WorkUnitState>> datasetWorkUnitStates : sourceState
        .getPreviousWorkUnitStatesByDatasetUrns().entrySet()) {

      LongWatermark tableWatermark = Collections.min(Lists.newArrayList(
          Iterables.transform(datasetWorkUnitStates.getValue(), new Function<WorkUnitState, LongWatermark>() {
            @Override
            public LongWatermark apply(WorkUnitState w) {
              return GSON.fromJson(w.getActualHighWatermark(), LongWatermark.class);
            }
          })));

      this.tableWatermarks.put(datasetWorkUnitStates.getKey(), tableWatermark);

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
}
