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
package gobblin.data.management.conversion.hive;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import gobblin.annotation.Alpha;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.entities.SerializableHivePartition;
import gobblin.data.management.conversion.hive.entities.SerializableHiveTable;
import gobblin.data.management.conversion.hive.events.EventConstants;
import gobblin.data.management.conversion.hive.extractor.HiveConvertExtractor;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import gobblin.data.management.conversion.hive.provider.UpdateNotFoundExecption;
import gobblin.data.management.conversion.hive.provider.UpdateProviderFactory;
import gobblin.data.management.conversion.hive.util.HiveSourceUtils;
import gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker;
import gobblin.data.management.conversion.hive.watermarker.TableLevelWatermarker;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.AutoReturnableObject;
import gobblin.util.HadoopUtils;
import gobblin.util.io.GsonInterfaceAdapter;


/**
 * <p>
 * A {@link Source} that creates generic workunits for a hive table or a hive partition.
 * </p>
 * <ul>
 *  <li>It uses the {@link HiveDatasetFinder} to find all hive tables and partitions
 *  <li>The update time of a hive {@link Table} or a hive {@link Partition} if found using {@link HiveUnitUpdateProvider}
 *  <li>The update time from the previous run is used as previous hive watermark.{@link HiveSourceWatermarker} is
 *  used to get previous hive watermarks
 * </ul>
 *
 *{@link WorkUnit}s are created if the previous high watermark of a {@link Partition}
 * or a {@link Table} are lower than the latest update time.
 *
 * <p>
 * The {@link WorkUnit}s contain a serialized json of the {@link SerializableHiveTable} or {@link SerializableHivePartition}
 * This is later deserialized by the extractor.
 * </p>
 */
@Slf4j
@SuppressWarnings("rawtypes")
@Alpha
public class HiveSource implements Source {

  public static final String HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY = "hive.source.maximum.lookbackDays";
  public static final int DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS = 30;

  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);

  private MetricContext metricContext;
  private EventSubmitter eventSubmitter;
  private AvroSchemaManager avroSchemaManager;

  @VisibleForTesting
  @Setter
  private HiveUnitUpdateProvider updateProvider;
  private HiveSourceWatermarker watermaker;
  private IterableDatasetFinder<HiveDataset> datasetFinder;
  private List<WorkUnit> workunits;
  private long maxLookBackTime;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {

      initialize(state);

      EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.FIND_HIVE_TABLES_EVENT);
      Iterator<HiveDataset> iterator = this.datasetFinder.getDatasetsIterator();

      while (iterator.hasNext()) {
        HiveDataset hiveDataset = iterator.next();
        try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {

          LongWatermark expectedDatasetHighWatermark = new LongWatermark(new DateTime().getMillis());
          log.debug(String.format("Processing dataset: %s", hiveDataset));

          // Create workunits for partitions
          if (HiveUtils.isPartitioned(hiveDataset.getTable())) {
            createWorkunitsForPartitionedTable(hiveDataset, client, expectedDatasetHighWatermark);
          } else {
            createPartitionForNonPartitionedTable(hiveDataset, expectedDatasetHighWatermark);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return this.workunits;
  }

  @VisibleForTesting
  public void initialize(SourceState state) throws IOException {

    this.updateProvider = UpdateProviderFactory.create(state);
    this.metricContext = Instrumented.getMetricContext(state, HiveSource.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    this.avroSchemaManager = new AvroSchemaManager(getSourceFs(), state);
    this.workunits = Lists.newArrayList();

    this.watermaker = new TableLevelWatermarker(state);
    EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.SETUP_EVENT);
    this.datasetFinder = new HiveDatasetFinder(getSourceFs(), state.getProperties(), this.eventSubmitter);
    int maxLookBackDays = state.getPropAsInt(HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS);
    this.maxLookBackTime = new DateTime().minusDays(maxLookBackDays).getMillis();
  }

  @VisibleForTesting
  public boolean shouldCreateWorkunitForPartition(Partition partition, long updateTime, LongWatermark lowWatermark) {
    // Do not create workunit if a partition was created before the lookbackTime
    DateTime createTime = new DateTime(TimeUnit.MILLISECONDS.convert(partition.getTPartition().getCreateTime(), TimeUnit.SECONDS));
    if (createTime.isBefore(this.maxLookBackTime)) {
      return false;
    }

    return new DateTime(updateTime).isAfter(lowWatermark.getValue());
  }

  @VisibleForTesting
  public boolean shouldCreateWorkunitForTable(long updateTime, LongWatermark lowWatermark) {
    return new DateTime(updateTime).isAfter(lowWatermark.getValue());
  }

  private void createPartitionForNonPartitionedTable(HiveDataset hiveDataset, LongWatermark expectedDatasetHighWatermark)
      throws IOException {
    // Create workunits for tables
    try {
      long updateTime = this.updateProvider.getUpdateTime(hiveDataset.getTable());

      LongWatermark lowWatermark = this.watermaker.getPreviousHighWatermark(hiveDataset.getTable());

      if (shouldCreateWorkunitForTable(updateTime, lowWatermark)) {

        log.debug(String.format("Processing table: %s", hiveDataset.getTable()));

        WorkUnit workUnit = WorkUnit.createEmpty();
        workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
        HiveSourceUtils.serializeTable(workUnit, hiveDataset.getTable(), this.avroSchemaManager);
        workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

        HiveSourceUtils.setTableSlaEventMetadata(workUnit, hiveDataset.getTable(), updateTime, lowWatermark.getValue());
        this.workunits.add(workUnit);
        log.debug(String.format("Workunit added for table: %s", workUnit));
      } else {
        log.info(String.format("Not creating workunit for table %s as updateTime %s is lesser than low watermark %s",
            hiveDataset.getTable().getCompleteName(), updateTime, lowWatermark.getValue()));
      }
    } catch (UpdateNotFoundExecption e) {
      log.info(String.format("Not Creating workunit for %s as update time was not found. %s", hiveDataset.getTable()
          .getCompleteName(), e.getMessage()));
    }
  }

  private void createWorkunitsForPartitionedTable(HiveDataset hiveDataset,
      AutoReturnableObject<IMetaStoreClient> client, LongWatermark expectedDatasetHighWatermark) throws IOException {

    List<Partition> sourcePartitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String> absent());

    for (Partition sourcePartition : sourcePartitions) {
      LongWatermark lowWatermark = watermaker.getPreviousHighWatermark(sourcePartition);

      try {
        long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
        if (shouldCreateWorkunitForPartition(sourcePartition, updateTime, lowWatermark)) {
          log.debug(String.format("Processing partition: %s", sourcePartition));

          WorkUnit workUnit = WorkUnit.createEmpty();
          workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
          HiveSourceUtils.serializeTable(workUnit, hiveDataset.getTable(), this.avroSchemaManager);
          HiveSourceUtils.serializePartition(workUnit, sourcePartition, this.avroSchemaManager);
          workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

          HiveSourceUtils.setPartitionSlaEventMetadata(workUnit, hiveDataset.getTable(), sourcePartition, updateTime,
              lowWatermark.getValue());
          workunits.add(workUnit);
          log.debug(String.format("Workunit added for partition: %s", workUnit));
        } else {
          // If watermark tracking at a partition level is necessary, create a dummy workunit for this partition here.
          log.info(String.format(
              "Not creating workunit for partition %s as updateTime %s is lesser than low watermark %s",
              sourcePartition.getCompleteName(), updateTime, lowWatermark.getValue()));
        }
      } catch (UpdateNotFoundExecption e) {
        log.info(String.format("Not Creating workunit for %s as update time was not found. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      }
    }
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) throws IOException {
    try {
      return new HiveConvertExtractor(state, getSourceFs());
    } catch (TException | HiveException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void shutdown(SourceState state) {
  }

  private static FileSystem getSourceFs() throws IOException {
    return FileSystem.get(HadoopUtils.newConfiguration());
  }
}
