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
package gobblin.data.management.conversion.hive.watermarker;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.source.HiveSource;
import gobblin.publisher.DataPublisher;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;


/**
 * An interface to read previous high watermarks and write new high watermarks to state.
 */
public interface HiveSourceWatermarker {

  /**
   * Get high watermark for a {@link Table}. This API is used by the {@link HiveSource} for Non Partitioned hive tables
   * @param table for which a high watermark needs to be returned
   */
  public LongWatermark getPreviousHighWatermark(Table table);

  /**
   * Get high watermark for a {@link Partition}. This API is used by the {@link HiveSource} for Partitioned hive tables
   * @param partition for which a high watermark needs to be returned
   */
  public LongWatermark getPreviousHighWatermark(Partition partition);

  /**
   * Get the expected high watermark for a {@link Table}. This API is used by the {@link HiveSource} to get Expected
   * high watermark for Non Partitioned hive tables
   *
   * @param table for which a high watermark needs to be returned
   * @param tableProcessTime time at which workunit creation started for this table
   */
  public LongWatermark getExpectedHighWatermark(Table table, long tableProcessTime);

  /**
   * Get the expected high watermark for a {@link Partition}.This API is used by the {@link HiveSource} for Partitioned hive tables
   *
   * @param partition for which a high watermark needs to be returned
   * @param tableProcessTime time at which workunit creation started for table this partition belongs to
   * @param partitionProcessTime time at which workunit creation started for this partition
   */
  public LongWatermark getExpectedHighWatermark(Partition partition, long tableProcessTime, long partitionProcessTime);

  /**
   * A callback method that {@link HiveSource} executes when workunit creation for a {@link Table} is started.
   *
   * @param table for which {@link WorkUnit}s will be created
   * @param tableProcessTime time at which this callback was called
   */
  public void onTableProcessBegin(Table table, long tableProcessTime);

  /**
   * A callback method that {@link HiveSource} executes when workunit creation for a {@link Partition} is started.
   *
   * @param partition for which {@link WorkUnit} will be created
   * @param partitionProcessTime time at which this callback was executed
   * @param partitionUpdateTime time at which this partition was updated
   */
  public void onPartitionProcessBegin(Partition partition, long partitionProcessTime, long partitionUpdateTime);

  /**
   * A callback method executed before a list of workunits is returned by the
   * {@link HiveSource#getWorkunits(gobblin.configuration.SourceState)} to the caller
   *
   * @param workunits constructed by {@link HiveSource#getWorkunits(gobblin.configuration.SourceState)}
   */
  public void onGetWorkunitsEnd(List<WorkUnit> workunits);

  /**
   * Sets the actual high watermark after data has been published by the {@link DataPublisher}
   * @param wus to set the watermark
   */
  public void setActualHighWatermark(WorkUnitState wus);
}
