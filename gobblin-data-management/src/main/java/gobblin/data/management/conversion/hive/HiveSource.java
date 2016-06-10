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

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.entities.SerializableHivePartition;
import gobblin.data.management.conversion.hive.entities.SerializableHiveTable;
import gobblin.data.management.conversion.hive.events.EventConstants;
import gobblin.data.management.conversion.hive.extractor.HiveConvertExtractor;
import gobblin.data.management.conversion.hive.provider.HdfsBasedUpdateProviderFactory;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProviderFactory;
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
public class HiveSource implements Source {

  private static final String OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS_KEY =
      "hive.unit.updateProviderFactory.class";
  private static final String DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS = HdfsBasedUpdateProviderFactory.class
      .getName();

  /**
   * Set this if you want to force bootstrap the low watermark for all datasets.
   */
  private static final String HIVE_SOURCE_BOOTSTRAP_LOW_WATERMARK_KEY = "hive.source.bootstrap.lowwatermark";

  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);

  public MetricContext metricContext;
  public EventSubmitter eventSubmitter;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    this.metricContext = Instrumented.getMetricContext(state, HiveSource.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();

    List<WorkUnit> workunits = Lists.newArrayList();

    Optional<LongWatermark> bootstrapLowWatermark = Optional.absent();

    if (state.contains(HIVE_SOURCE_BOOTSTRAP_LOW_WATERMARK_KEY)) {
      bootstrapLowWatermark = Optional.of(new LongWatermark(state.getPropAsLong(HIVE_SOURCE_BOOTSTRAP_LOW_WATERMARK_KEY)));
    }

    try {

      // Initialize
      EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.SETUP_EVENT);
      HiveSourceWatermarker watermaker = new TableLevelWatermarker(state);
      HiveUnitUpdateProviderFactory updateProviderFactory =
          GobblinConstructorUtils.invokeConstructor(HiveUnitUpdateProviderFactory.class, state.getProp(
              OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS_KEY, DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS));
      HiveUnitUpdateProvider updateProvider = updateProviderFactory.create(state);

      IterableDatasetFinder<HiveDataset> datasetFinder =
          new HiveDatasetFinder(getSourceFs(), state.getProperties(), this.eventSubmitter);

      AvroSchemaManager avroSchemaManager = new AvroSchemaManager(getSourceFs(), state);

      // Find hive tables
      EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.FIND_HIVE_TABLES_EVENT);
      Iterator<HiveDataset> iterator = datasetFinder.getDatasetsIterator();

      while (iterator.hasNext()) {
        HiveDataset hiveDataset = iterator.next();
        try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {

          LongWatermark expectedDatasetHighWatermark = new LongWatermark(new DateTime().getMillis());
          log.debug(String.format("Processing dataset: %s", hiveDataset));

          // Create workunits for partitions
          if (HiveUtils.isPartitioned(hiveDataset.getTable())) {
            List<Partition> sourcePartitions =
                HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String>absent());

            for (Partition sourcePartition : sourcePartitions) {
              LongWatermark lowWatermark = watermaker.getPreviousHighWatermark(sourcePartition);
              if (bootstrapLowWatermark.isPresent() &&
                  Long.compare(bootstrapLowWatermark.get().getValue(), lowWatermark.getValue()) > 0) {
                lowWatermark = bootstrapLowWatermark.get();
              }

              long updateTime = updateProvider.getUpdateTime(sourcePartition);

              if (Long.compare(updateTime, lowWatermark.getValue()) > 0) {

                log.debug(String.format("Processing partition: %s", sourcePartition));

                WorkUnit workUnit = WorkUnit.createEmpty();
                workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
                HiveSourceUtils.serializeTable(workUnit, hiveDataset.getTable(), avroSchemaManager);
                HiveSourceUtils.serializePartition(workUnit, sourcePartition, avroSchemaManager);
                workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

                HiveSourceUtils
                    .setPartitionSlaEventMetadata(workUnit, hiveDataset.getTable(), sourcePartition, updateTime,
                        lowWatermark.getValue());
                workunits.add(workUnit);
                log.debug(String.format("Workunit added for partition: %s", workUnit));
              } else {
                // If watermark tracking at a partition level is necessary, create a dummy workunit for this partition here.
                log.info(String
                    .format("Not creating workunit for partition %s as updateTime %s is lesser than low watermark %s",
                        sourcePartition.getCompleteName(), updateTime, lowWatermark.getValue()));
              }
            }
          } else {

            // Create workunits for tables
            long updateTime = updateProvider.getUpdateTime(hiveDataset.getTable());

            LongWatermark lowWatermark = watermaker.getPreviousHighWatermark(hiveDataset.getTable());
            if (bootstrapLowWatermark.isPresent() &&
                Long.compare(bootstrapLowWatermark.get().getValue(), lowWatermark.getValue()) > 0) {
              lowWatermark = bootstrapLowWatermark.get();
            }

            if (Long.compare(updateTime, lowWatermark.getValue()) > 0) {

              log.debug(String.format("Processing table: %s", hiveDataset.getTable()));

              WorkUnit workUnit = WorkUnit.createEmpty();
              workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
              HiveSourceUtils.serializeTable(workUnit, hiveDataset.getTable(), avroSchemaManager);
              workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

              HiveSourceUtils
                  .setTableSlaEventMetadata(workUnit, hiveDataset.getTable(), updateTime, lowWatermark.getValue());
              workunits.add(workUnit);
              log.debug(String.format("Workunit added for table: %s", workUnit));
            } else {
              log.info(String
                  .format("Not creating workunit for table %s as updateTime %s is lesser than low watermark %s",
                      hiveDataset.getTable().getCompleteName(), updateTime, lowWatermark.getValue()));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return workunits;
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
