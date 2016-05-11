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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveTable;
import gobblin.hive.metastore.HiveMetaStoreUtils;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.workunit.WorkUnit;
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
 * The {@link WorkUnit}s contain a serialized json of the {@link HiveTable} or {@link HivePartition} at {@value #HIVE_UNIT_SERIALIZED_KEY}
 * This is later deserialized by the extractor.
 * </p>
 */
@Slf4j
@SuppressWarnings("rawtypes")
public class HiveSource implements Source {

  private static final String OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS_KEY =
      "hive.unit.updateProviderFactory.class";
  private static final String DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS = HdfsBasedUpdateProviderFactory.class.getName();

  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);

  public static final String HIVE_UNIT_SERIALIZED_KEY = "hive.unit.serialized";
  public static final String PARTITION_COMPLETE_NAME_KEY = "hive.partition.completeName";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    List<WorkUnit> workunits = Lists.newArrayList();
    try {

      // Initialize
      HiveSourceWatermarker watermaker = new TableLevelWatermarker(state);
      HiveUnitUpdateProviderFactory updateProviderFactory = GobblinConstructorUtils.invokeConstructor(HiveUnitUpdateProviderFactory.class, state.getProp(OPTIONAL_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS_KEY, DEFAULT_HIVE_UNIT_UPDATE_PROVIDER_FACTORY_CLASS));
      HiveUnitUpdateProvider updateProvider = updateProviderFactory.create(state);
      IterableDatasetFinder<HiveDataset> datasetFinder = new HiveDatasetFinder(getSourceFs(), state.getProperties());

      // Find hive tables
      Iterator<HiveDataset> iterator = datasetFinder.getDatasetsIterator();

      while (iterator.hasNext()) {
        HiveDataset hiveDataset = iterator.next();
        LongWatermark expectedDatasetHighWatermark = new LongWatermark(new DateTime().getMillis());

        // Create workunits for partitions
        if (HiveUtils.isPartitioned(hiveDataset.getTable())) {
          List<Partition> sourcePartitions =
              HiveUtils.getPartitions(hiveDataset.getClientPool().getClient().get(), hiveDataset.getTable(),
                  Optional.<String> absent());

          for (Partition sourcePartition : sourcePartitions) {
            LongWatermark lowWatermark = watermaker.getPreviousHighWatermark(sourcePartition);
            long updateTime = updateProvider.getUpdateTime(sourcePartition);
            if (Long.compare(updateTime, lowWatermark.getValue()) > 0) {

              HivePartition hivePartition =
                  HiveMetaStoreUtils.getHivePartition(sourcePartition.getTPartition());
              WorkUnit workUnit = WorkUnit.createEmpty();

              workUnit.setProp(HIVE_UNIT_SERIALIZED_KEY, GENERICS_AWARE_GSON.toJson(hivePartition, HivePartition.class));
              workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));
              workUnit.setProp(PARTITION_COMPLETE_NAME_KEY, sourcePartition.getCompleteName());
              workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
              workunits.add(workUnit);
            } else {
              // If watermark tracking at a partition level is necessary, create a dummy workunit for this partition here.
              log.info(String.format(
                  "Not creating workunit for partition %s as updateTime %s is lesser than low watermark %s",
                  sourcePartition.getCompleteName(), updateTime, lowWatermark.getValue()));
            }
          }
        } else {

          // Create workunits for tables
          long updateTime = updateProvider.getUpdateTime(hiveDataset.getTable());
          LongWatermark lowWatermark = watermaker.getPreviousHighWatermark(hiveDataset.getTable());
          if (Long.compare(updateTime, lowWatermark.getValue()) > 0) {
            HiveTable hiveTable = HiveMetaStoreUtils.getHiveTable(hiveDataset.getTable().getTTable());
            WorkUnit workUnit = WorkUnit.createEmpty();
            workUnit.setProp(HIVE_UNIT_SERIALIZED_KEY, GENERICS_AWARE_GSON.toJson(hiveTable, HiveTable.class));
            workUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));
            workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, hiveDataset.getTable().getCompleteName());
            workunits.add(workUnit);
          } else {
            log.info(String.format(
                "Not creating workunit for table %s as updateTime %s is lesser than low watermark %s", hiveDataset
                    .getTable().getCompleteName(), updateTime, lowWatermark.getValue()));
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
    return new HiveConvertExtractor(state, getSourceFs());
  }

  @Override
  public void shutdown(SourceState state) {
  }

  private FileSystem getSourceFs() throws IOException {
    return FileSystem.get(HadoopUtils.newConfiguration());
  }
}
