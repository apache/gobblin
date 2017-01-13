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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


import javax.annotation.Nonnull;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.converter.AbstractAvroToOrcConverter;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import gobblin.data.management.conversion.hive.provider.UpdateNotFoundException;
import gobblin.data.management.conversion.hive.provider.UpdateProviderFactory;
import gobblin.data.management.conversion.hive.source.HiveSource;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.source.extractor.Watermark;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.AutoReturnableObject;

import javax.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link HiveSourceWatermarker} that maintains watermarks for each {@link Partition}. For every {@link Table} it creates
 *  a NoOp {@link WorkUnit} that stores watermarks for all its {@link Partition}s. The Noop workunit is identified by the
 *  property {@link #IS_WATERMARK_WORKUNIT_KEY} being set to true.
 *  <p>
 *  The watermark used is the update time of a {@link Partition} (for partitioned tables)
 *  or update time of a {@link Table} (for non partitioned tables).
 *  </p>
 *  <p>
 *  The watermark is stored as a {@link MultiKeyValueLongWatermark} which is an extension to {@link Map} that implements
 *  gobblin's {@link Watermark}. The key is a {@link Partition} identifier and value is the watermark for this {@link Partition}
 *  </p>
 *  <p>
 *  Watermarks for all {@link Partition}s modified after {@link HiveSource#HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY} are retained.
 *  The default is {@link HiveSource#DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS}. If a previous watermark is not found for
 *  as partition, it returns 0 as the watermark
 *  </p>
 *  <p>
 *  Watermark workunits are not created for non partitioned tables
 *  </p>
 */
@Slf4j
public class PartitionLevelWatermarker implements HiveSourceWatermarker {

  public static final String IS_WATERMARK_WORKUNIT_KEY = "hive.source.watermark.isWatermarkWorkUnit";

  private static final Joiner PARTITION_VALUES_JOINER = Joiner.on(",");

  static final Predicate<WorkUnitState> WATERMARK_WORKUNIT_PREDICATE = new Predicate<WorkUnitState>() {
    @Override
    public boolean apply(@Nonnull WorkUnitState input) {
      return input.contains(IS_WATERMARK_WORKUNIT_KEY);
    }
  };

  @Setter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private long leastWatermarkToPersistInState;
  // Keep an additional 2 days of updates
  private static final int BUFFER_WATERMARK_DAYS_TO_PERSIST = 2;

  /**
   * Watermarks from previous state
   */
  @Getter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private final TableWatermarks previousWatermarks;

  /**
   * Current expected watermarks
   */
  @Getter(AccessLevel.PACKAGE)
  @VisibleForTesting
  private final TableWatermarks expectedHighWatermarks;

  private final HiveMetastoreClientPool pool;
  /**
   * Delegates watermarking logic to {@link TableLevelWatermarker} for Non partitioned tables
   */
  private final TableLevelWatermarker tableLevelWatermarker;

  private final HiveUnitUpdateProvider updateProvider;

  /**
   * Reads and initialized the previous high watermarks from {@link SourceState#getPreviousDatasetStatesByUrns()}
   */
  public PartitionLevelWatermarker(State state) {

    this.expectedHighWatermarks = new TableWatermarks();
    this.previousWatermarks = new TableWatermarks();
    this.tableLevelWatermarker = new TableLevelWatermarker(state);
    this.updateProvider = UpdateProviderFactory.create(state);
    try {
      this.pool =
          HiveMetastoreClientPool.get(state.getProperties(),
              Optional.fromNullable(state.getProp(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
    } catch (IOException e) {
      throw new RuntimeException("Could not initialize metastore client pool", e);
    }

    int maxLookBackDays =
        state.getPropAsInt(HiveSource.HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY,
            HiveSource.DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS) + BUFFER_WATERMARK_DAYS_TO_PERSIST;

    this.leastWatermarkToPersistInState = new DateTime().minusDays(maxLookBackDays).getMillis();

    // Load previous watermarks in case of sourceState
    if (state instanceof SourceState) {
      SourceState sourceState = (SourceState) state;

      for (Map.Entry<String, Iterable<WorkUnitState>> datasetWorkUnitStates : sourceState
          .getPreviousWorkUnitStatesByDatasetUrns().entrySet()) {

        List<WorkUnitState> watermarkWorkUnits =
            Lists.newArrayList(Iterables.filter(datasetWorkUnitStates.getValue(), WATERMARK_WORKUNIT_PREDICATE));

        if (watermarkWorkUnits.isEmpty()) {
          log.info(String.format("No previous partition watermarks for table %s", datasetWorkUnitStates.getKey()));
          continue;
        } else if (watermarkWorkUnits.size() > 1) {
          throw new IllegalStateException(
              String
                  .format(
                      "Each table should have only 1 watermark workunit that contains watermarks for all its partitions. Found %s",
                      watermarkWorkUnits.size()));
        } else {
          MultiKeyValueLongWatermark multiKeyValueLongWatermark =
              watermarkWorkUnits.get(0).getActualHighWatermark(MultiKeyValueLongWatermark.class);
          if (multiKeyValueLongWatermark != null) {
            this.previousWatermarks.setPartitionWatermarks(datasetWorkUnitStates.getKey(),
                multiKeyValueLongWatermark.getWatermarks());
          } else {
            log.warn(String.format("Previous workunit for %s has %s set but null MultiKeyValueLongWatermark found",
                datasetWorkUnitStates.getKey(), IS_WATERMARK_WORKUNIT_KEY));
          }
        }
      }

      log.debug("Loaded partition watermarks from previous state " + this.previousWatermarks);

      for (String tableKey : this.previousWatermarks.keySet()) {
        this.expectedHighWatermarks.setPartitionWatermarks(tableKey,
            Maps.newHashMap(this.previousWatermarks.getPartitionWatermarks(tableKey)));
      }
    }

  }

  /**
   * Initializes the expected high watermarks for a {@link Table}
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#onTableProcessBegin(org.apache.hadoop.hive.ql.metadata.Table, long)
   */
  @Override
  public void onTableProcessBegin(Table table, long tableProcessTime) {

    Preconditions.checkNotNull(table);

    if (!this.expectedHighWatermarks.hasPartitionWatermarks(tableKey(table))) {
      this.expectedHighWatermarks.setPartitionWatermarks(tableKey(table), Maps.<String, Long> newHashMap());
    }
  }

  /**
   * Adds an expected high watermark for this {@link Partition}. Also removes any watermarks for partitions being replaced.
   * Replace partitions are read using partition parameter {@link AbstractAvroToOrcConverter#REPLACED_PARTITIONS_HIVE_METASTORE_KEY}.
   * Uses the <code>partitionUpdateTime</code> as the high watermark for this <code>partition</code>
   *
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#onPartitionProcessBegin(org.apache.hadoop.hive.ql.metadata.Partition, long, long)
   */
  @Override
  public void onPartitionProcessBegin(Partition partition, long partitionProcessTime, long partitionUpdateTime) {

    Preconditions.checkNotNull(partition);
    Preconditions.checkNotNull(partition.getTable());

    if (!this.expectedHighWatermarks.hasPartitionWatermarks(tableKey(partition.getTable()))) {
      throw new IllegalStateException(String.format(
          "onPartitionProcessBegin called before onTableProcessBegin for table: %s, partitions: %s",
          tableKey(partition.getTable()), partitionKey(partition)));
    }

    // Remove dropped partitions
    Collection<String> droppedPartitions =
        Collections2.transform(AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(partition),
            new Function<Map<String, String>, String>() {
              @Override
              public String apply(Map<String, String> input) {
                return PARTITION_VALUES_JOINER.join(input.values());
              }
            });

    this.expectedHighWatermarks.removePartitionWatermarks(tableKey(partition.getTable()), droppedPartitions);
    this.expectedHighWatermarks.addPartitionWatermark(tableKey(partition.getTable()), partitionKey(partition),
        partitionUpdateTime);
  }

  /**
   * Delegates to {@link TableLevelWatermarker#getPreviousHighWatermark(Table)}
   *
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#getPreviousHighWatermark(org.apache.hadoop.hive.ql.metadata.Table)
   */
  @Override
  public LongWatermark getPreviousHighWatermark(Table table) {
    return this.tableLevelWatermarker.getPreviousHighWatermark(table);
  }

  /**
   * Return the previous high watermark if found in previous state. Else returns 0
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#getPreviousHighWatermark(org.apache.hadoop.hive.ql.metadata.Partition)
   */
  @Override
  public LongWatermark getPreviousHighWatermark(Partition partition) {
    if (this.previousWatermarks.hasPartitionWatermarks(tableKey(partition.getTable()))) {

      // If partition has a watermark return.
      if (this.previousWatermarks.get(tableKey(partition.getTable())).containsKey(partitionKey(partition))) {
        return new LongWatermark(this.previousWatermarks.getPartitionWatermark(tableKey(partition.getTable()),
            partitionKey(partition)));
      }
    }
    return new LongWatermark(0);

  }

  /**
   * Adds watermark workunits to <code>workunits</code>. A watermark workunit is a dummy workunit that is skipped by extractor/converter/writer.
   * It stores a map of watermarks. The map has one entry per partition with partition watermark as value.
   * <ul>
   * <li>Add one NoOp watermark workunit for each {@link Table}
   * <li>The workunit has an identifier property {@link #IS_WATERMARK_WORKUNIT_KEY} set to true.
   * <li>Watermarks for all {@link Partition}s that belong to this {@link Table} are added as {@link Map}
   * <li>A maximum of {@link #maxPartitionsPerDataset} are persisted. Watermarks are ordered by most recently modified {@link Partition}s
   *
   * </ul>
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#onGetWorkunitsEnd(java.util.List)
   */
  @Override
  public void onGetWorkunitsEnd(List<WorkUnit> workunits) {
    try (AutoReturnableObject<IMetaStoreClient> client = this.pool.getClient()) {
      for (Map.Entry<String, Map<String, Long>> tableWatermark : this.expectedHighWatermarks.entrySet()) {

        String tableKey = tableWatermark.getKey();
        Map<String, Long> partitionWatermarks = tableWatermark.getValue();

        // Watermark workunits are required only for Partitioned tables
        // tableKey is table complete name in the format db@table
        if (!HiveUtils.isPartitioned(new org.apache.hadoop.hive.ql.metadata.Table(client.get().getTable(
            tableKey.split("@")[0], tableKey.split("@")[1])))) {
          continue;
        }
        // We only keep watermarks for partitions that were updated after leastWatermarkToPersistInState
        Map<String, Long> expectedPartitionWatermarks =
            ImmutableMap.copyOf(Maps.filterEntries(partitionWatermarks, new Predicate<Map.Entry<String, Long>>() {

              @Override
              public boolean apply(@Nonnull Map.Entry<String, Long> input) {
                return Long.compare(input.getValue(), PartitionLevelWatermarker.this.leastWatermarkToPersistInState) >= 0;
              }
            }));

        // Create dummy workunit to track all the partition watermarks for this table
        WorkUnit watermarkWorkunit = WorkUnit.createEmpty();
        watermarkWorkunit.setProp(IS_WATERMARK_WORKUNIT_KEY, true);
        watermarkWorkunit.setProp(ConfigurationKeys.DATASET_URN_KEY, tableKey);

        watermarkWorkunit.setWatermarkInterval(new WatermarkInterval(new MultiKeyValueLongWatermark(
            this.previousWatermarks.get(tableKey)), new MultiKeyValueLongWatermark(expectedPartitionWatermarks)));

        workunits.add(watermarkWorkunit);
      }
    } catch (IOException | TException e) {
      Throwables.propagate(e);
    }
  }

  /**
   *
   * {@inheritDoc}
   *
   * Uses the <code>table</code>'s modified time as watermark. The modified time is read using
   * {@link HiveUnitUpdateProvider#getUpdateTime(Table)}
   * @throws UpdateNotFoundException if there was an error fetching update time using {@link HiveUnitUpdateProvider#getUpdateTime(Table)}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#getExpectedHighWatermark(org.apache.hadoop.hive.ql.metadata.Table, long)
   */
  @Override
  public LongWatermark getExpectedHighWatermark(Table table, long tableProcessTime) {
    return new LongWatermark(this.updateProvider.getUpdateTime(table));
  }

  /**
   * Get the expected high watermark for this partition
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#getExpectedHighWatermark(org.apache.hadoop.hive.ql.metadata.Partition, long, long)
   */
  @Override
  public LongWatermark getExpectedHighWatermark(Partition partition, long tableProcessTime, long partitionProcessTime) {
    return new LongWatermark(this.expectedHighWatermarks.getPartitionWatermark(tableKey(partition.getTable()),
        partitionKey(partition)));
  }

  /**
   * Sets the actual high watermark by reading the expected high watermark
   * {@inheritDoc}
   * @see gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker#setActualHighWatermark(gobblin.configuration.WorkUnitState)
   */
  @Override
  public void setActualHighWatermark(WorkUnitState wus) {
    if (Boolean.valueOf(wus.getPropAsBoolean(IS_WATERMARK_WORKUNIT_KEY))) {
      wus.setActualHighWatermark(wus.getWorkunit().getExpectedHighWatermark(MultiKeyValueLongWatermark.class));
    } else {
      wus.setActualHighWatermark(wus.getWorkunit().getExpectedHighWatermark(LongWatermark.class));
    }
  }

  @VisibleForTesting
  public static String tableKey(Table table) {
    return table.getCompleteName();
  }

  @VisibleForTesting
  public static String partitionKey(Partition partition) {
    return PARTITION_VALUES_JOINER.join(partition.getValues());
  }

  /**
   * An extension to standard java map with some accessors
   */
  @VisibleForTesting
  static class TableWatermarks extends ConcurrentHashMap<String, Map<String, Long>> {

    private static final long serialVersionUID = 1L;

    public TableWatermarks() {
      super();
    }

    void setPartitionWatermarks(String tableKey, Map<String, Long> partitionWatermarks) {
      this.put(tableKey, partitionWatermarks);
    }

    boolean hasPartitionWatermarks(String tableKey) {
      return this.containsKey(tableKey);
    }

    void removePartitionWatermarks(String tableKey, Collection<String> partitionKeys) {
      this.get(tableKey).keySet().removeAll(partitionKeys);
    }

    void addPartitionWatermark(String tableKey, String partitionKey, Long watermark) {
      this.get(tableKey).put(partitionKey, watermark);
    }

    Long getPartitionWatermark(String tableKey, String partitionKey) {
      return this.get(tableKey).get(partitionKey);
    }

    Map<String, Long> getPartitionWatermarks(String tableKey) {
      return this.get(tableKey);
    }
  }

  /**
   * Factory to create a {@link PartitionLevelWatermarker}
   */
  public static class Factory implements HiveSourceWatermarkerFactory {

    @Override
    public PartitionLevelWatermarker createFromState(State state) {
      return new PartitionLevelWatermarker(state);
    }
  }
}
