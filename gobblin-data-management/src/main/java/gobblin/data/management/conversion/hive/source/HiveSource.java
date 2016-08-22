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
package gobblin.data.management.conversion.hive.source;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.AvroSchemaManager;
import gobblin.data.management.conversion.hive.events.EventConstants;
import gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import gobblin.data.management.conversion.hive.extractor.HiveConvertExtractor;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import gobblin.data.management.conversion.hive.provider.UpdateNotFoundException;
import gobblin.data.management.conversion.hive.provider.UpdateProviderFactory;
import gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker;
import gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarkerFactory;
import gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
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
import gobblin.util.reflection.GobblinConstructorUtils;


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
  public static final int DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS = 3;

  public static final String HIVE_SOURCE_DATASET_FINDER_CLASS_KEY = "hive.dataset.finder.class";
  public static final String DEFAULT_HIVE_SOURCE_DATASET_FINDER_CLASS = HiveDatasetFinder.class.getName();

  public static final String DISTCP_REGISTRATION_GENERATION_TIME_KEY = "registrationGenerationTimeMillis";
  public static final String HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY = "hive.source.watermarker.factoryClass";
  public static final String DEFAULT_HIVE_SOURCE_WATERMARKER_FACTORY_CLASS = PartitionLevelWatermarker.Factory.class.getName();

  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);

  private MetricContext metricContext;
  private EventSubmitter eventSubmitter;
  private AvroSchemaManager avroSchemaManager;

  private HiveUnitUpdateProvider updateProvider;
  private HiveSourceWatermarker watermarker;
  private IterableDatasetFinder<HiveDataset> datasetFinder;
  private List<WorkUnit> workunits;
  private long maxLookBackTime;
  private long beginGetWorkunitsTime;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    try {
      this.beginGetWorkunitsTime = System.currentTimeMillis();

      initialize(state);

      EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.CONVERSION_FIND_HIVE_TABLES_EVENT);
      Iterator<HiveDataset> iterator = this.datasetFinder.getDatasetsIterator();

      while (iterator.hasNext()) {
        HiveDataset hiveDataset = iterator.next();
        try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {

          log.debug(String.format("Processing dataset: %s", hiveDataset));

          // Create workunits for partitions
          if (HiveUtils.isPartitioned(hiveDataset.getTable())) {
            createWorkunitsForPartitionedTable(hiveDataset, client);
          } else {
            createWorkunitForNonPartitionedTable(hiveDataset);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int realWorkunits = this.workunits.size();

    this.watermarker.onGetWorkunitsEnd(this.workunits);

    log.info(String.format("Created %s real workunits and %s watermark workunits", realWorkunits,
        (this.workunits.size() - realWorkunits)));

    return this.workunits;
  }

  @VisibleForTesting
  public void initialize(SourceState state) throws IOException {

    this.updateProvider = UpdateProviderFactory.create(state);
    this.metricContext = Instrumented.getMetricContext(state, HiveSource.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    this.avroSchemaManager = new AvroSchemaManager(getSourceFs(), state);
    this.workunits = Lists.newArrayList();

    this.watermarker =
        GobblinConstructorUtils.invokeConstructor(HiveSourceWatermarkerFactory.class,
            state.getProp(HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY, DEFAULT_HIVE_SOURCE_WATERMARKER_FACTORY_CLASS))
            .createFromState(state);

    EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.CONVERSION_SETUP_EVENT);
    this.datasetFinder = GobblinConstructorUtils.invokeConstructor(HiveDatasetFinder.class,
        state.getProp(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY, DEFAULT_HIVE_SOURCE_DATASET_FINDER_CLASS), getSourceFs(), state.getProperties(),
        this.eventSubmitter);
    int maxLookBackDays = state.getPropAsInt(HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS);
    this.maxLookBackTime = new DateTime().minusDays(maxLookBackDays).getMillis();
  }


  private void createWorkunitForNonPartitionedTable(HiveDataset hiveDataset) throws IOException {
    // Create workunits for tables
    try {

      long tableProcessTime = new DateTime().getMillis();

      long updateTime = this.updateProvider.getUpdateTime(hiveDataset.getTable());

      this.watermarker.onTableProcessBegin(hiveDataset.getTable(), tableProcessTime);

      LongWatermark lowWatermark = this.watermarker.getPreviousHighWatermark(hiveDataset.getTable());

      if (shouldCreateWorkunit(hiveDataset.getTable().getTTable().getCreateTime(), updateTime, lowWatermark)) {

        log.debug(String.format("Processing table: %s", hiveDataset.getTable()));
        LongWatermark expectedDatasetHighWatermark =
            this.watermarker.getExpectedHighWatermark(hiveDataset.getTable(), tableProcessTime);
        HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(hiveDataset);
        hiveWorkUnit.setTableSchemaUrl(this.avroSchemaManager.getSchemaUrl(hiveDataset.getTable()));
        hiveWorkUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

        EventWorkunitUtils.setTableSlaEventMetadata(hiveWorkUnit, hiveDataset.getTable(), updateTime, lowWatermark.getValue(),
            this.beginGetWorkunitsTime);
        this.workunits.add(hiveWorkUnit);
        log.debug(String.format("Workunit added for table: %s", hiveWorkUnit));

      } else {
        log.info(String.format("Not creating workunit for table %s as updateTime %s is not greater than low watermark %s",
            hiveDataset.getTable().getCompleteName(), updateTime, lowWatermark.getValue()));
      }
    } catch (UpdateNotFoundException e) {
      log.error(String.format("Not Creating workunit for %s as update time was not found. %s", hiveDataset.getTable()
          .getCompleteName(), e.getMessage()));
    }
  }

  private void createWorkunitsForPartitionedTable(HiveDataset hiveDataset, AutoReturnableObject<IMetaStoreClient> client) throws IOException {

    long tableProcessTime = new DateTime().getMillis();
    this.watermarker.onTableProcessBegin(hiveDataset.getTable(), tableProcessTime);

    List<Partition> sourcePartitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(),
        Optional.<String>absent());

    for (Partition sourcePartition : sourcePartitions) {

      if (isOlderThanLookback(sourcePartition)) {
        continue;
      }

      LongWatermark lowWatermark = watermarker.getPreviousHighWatermark(sourcePartition);

      try {
        long updateTime = this.updateProvider.getUpdateTime(sourcePartition);

        if (shouldCreateWorkunit(sourcePartition, lowWatermark)) {
          log.debug(String.format("Processing partition: %s", sourcePartition));

          long partitionProcessTime = new DateTime().getMillis();
          this.watermarker.onPartitionProcessBegin(sourcePartition, partitionProcessTime, updateTime);

          LongWatermark expectedPartitionHighWatermark = this.watermarker.getExpectedHighWatermark(sourcePartition,
              tableProcessTime, partitionProcessTime);

          HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(hiveDataset);
          hiveWorkUnit.setTableSchemaUrl(this.avroSchemaManager.getSchemaUrl(hiveDataset.getTable()));
          hiveWorkUnit.setPartitionSchemaUrl(this.avroSchemaManager.getSchemaUrl(sourcePartition));
          hiveWorkUnit.setPartitionName(sourcePartition.getName());
          hiveWorkUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedPartitionHighWatermark));

          EventWorkunitUtils.setPartitionSlaEventMetadata(hiveWorkUnit, hiveDataset.getTable(), sourcePartition, updateTime,
              lowWatermark.getValue(), this.beginGetWorkunitsTime);
          workunits.add(hiveWorkUnit);
          log.info(String.format("Creating workunit for partition %s as updateTime %s is greater than low watermark %s",
              sourcePartition.getCompleteName(), updateTime, lowWatermark.getValue()));
        } else {
          // If watermark tracking at a partition level is necessary, create a dummy workunit for this partition here.
          log.info(String.format(
              "Not creating workunit for partition %s as updateTime %s is lesser than low watermark %s",
              sourcePartition.getCompleteName(), updateTime, lowWatermark.getValue()));
        }
      } catch (UpdateNotFoundException e) {
        log.error(String.format("Not Creating workunit for %s as update time was not found. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      }
    }
  }

  protected boolean shouldCreateWorkunit(Partition sourcePartition, LongWatermark lowWatermark) throws UpdateNotFoundException {
    long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
    long createTime = getCreateTime(sourcePartition);
    return shouldCreateWorkunit(createTime, updateTime, lowWatermark);
  }

  /**
   * Check if workunit needs to be created. Returns <code>true</code> If either the <code>createTime</code> or the
   * <code>updateTime</code> is greater than the <code>lowWatermark</code>
   */
  protected boolean shouldCreateWorkunit(long createTime, long updateTime, LongWatermark lowWatermark) {
    /*
     * [2016-08-08]
     * Need to check both updateTime and createTime.
     *
     * Distcp ng published data with an older modified time. In Distcp-ng, the modified time of data on HDFS is the time
     * when data was created in staging and not final publish path, hence this time difference between HDFS mod time and
     * hive registration time can be in the order of minutes. This may lead to missing updates by the Avro to ORC job.
     * E.g. Let's say table level watermark for a dataset is 1:30 from the previous run.
     * Now distcp-np published data for a partition at 1:33 with an HDFS modTime of 1:27.
     * The watermark is 1:30 and the update is at 1:27 hence a workunit is not created for this partition.
     *
     * Summary -
     * - Check for update time is required when new files are added to existing partitions
     * - Check for create time is required when a new partition is created both in hive and HDFS
     */
    return new DateTime(updateTime).isAfter(lowWatermark.getValue()) || new DateTime(createTime).isAfter(createTime);
  }

  /**
   * Do not create workunit if a partition was created before the lookbackTime
   */
  @VisibleForTesting
  public boolean isOlderThanLookback(Partition partition) {
    return new DateTime(getCreateTime(partition)).isBefore(this.maxLookBackTime);
  }

  @VisibleForTesting
  public static long getCreateTime(Partition partition) {
    // If create time is set, use it.
    // .. this is always set if HiveJDBC or Hive mestastore is used to create partition.
    // .. it might not be set (ie. equals 0) if Thrift API call is used to create partition.
    if (partition.getTPartition().getCreateTime() > 0) {
      return TimeUnit.MILLISECONDS.convert(partition.getTPartition().getCreateTime(), TimeUnit.SECONDS);
    }
    // Try to use distcp-ng registration generation time if it is available
    else if (partition.getTPartition().isSetParameters() && partition.getTPartition().getParameters().containsKey(DISTCP_REGISTRATION_GENERATION_TIME_KEY)) {

      log.debug("Did not find createTime in Hive partition, used distcp registration generation time.");
      return Long.parseLong(partition.getTPartition().getParameters().get(DISTCP_REGISTRATION_GENERATION_TIME_KEY));
    } else {
      throw new IllegalStateException(String.format("Could not find create time in hive metastore for partition %s",
          partition.getCompleteName()));
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
