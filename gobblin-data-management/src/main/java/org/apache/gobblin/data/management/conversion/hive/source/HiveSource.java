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
package org.apache.gobblin.data.management.conversion.hive.source;

import com.google.common.util.concurrent.UncheckedExecutionException;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.conversion.hive.avro.AvroSchemaManager;
import org.apache.gobblin.data.management.conversion.hive.avro.SchemaNotFoundException;
import org.apache.gobblin.data.management.conversion.hive.events.EventConstants;
import org.apache.gobblin.data.management.conversion.hive.events.EventWorkunitUtils;
import org.apache.gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import org.apache.gobblin.data.management.conversion.hive.provider.UpdateNotFoundException;
import org.apache.gobblin.data.management.conversion.hive.provider.UpdateProviderFactory;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarker;
import org.apache.gobblin.data.management.conversion.hive.watermarker.HiveSourceWatermarkerFactory;
import org.apache.gobblin.data.management.conversion.hive.watermarker.PartitionLevelWatermarker;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.data.management.copy.hive.HiveUtils;
import org.apache.gobblin.data.management.copy.hive.filter.LookbackPartitionFilterGenerator;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.data.management.conversion.hive.extractor.HiveBaseExtractorFactory;
import org.apache.gobblin.data.management.conversion.hive.extractor.HiveConvertExtractorFactory;


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

  public static final String HIVE_SOURCE_EXTRACTOR_TYPE = "hive.source.extractorType";
  public static final String DEFAULT_HIVE_SOURCE_EXTRACTOR_TYPE = HiveConvertExtractorFactory.class.getName();

  public static final String HIVE_SOURCE_CREATE_WORKUNITS_FOR_PARTITIONS = "hive.source.createWorkunitsForPartitions";
  public static final boolean DEFAULT_HIVE_SOURCE_CREATE_WORKUNITS_FOR_PARTITIONS = true;

  public static final String HIVE_SOURCE_FS_URI = "hive.source.fs.uri";

  /***
   * Comma separated list of keywords to look for in path of table (in non-partitioned case) / partition (in partitioned case)
   * and if the keyword is found then ignore the table / partition from processing.
   *
   * This is useful in scenarios like:
   * - when the user wants to ignore hourly partitions and only process daily partitions and only way to identify that
   *   is by the path both store there data. eg: /foo/bar/2016/12/01/hourly/00
   * - when the user wants to ignore partitions that refer to partitions pointing to /tmp location (for debug reasons)
   */
  public static final String HIVE_SOURCE_IGNORE_DATA_PATH_IDENTIFIER_KEY = "hive.source.ignoreDataPathIdentifier";
  public static final String DEFAULT_HIVE_SOURCE_IGNORE_DATA_PATH_IDENTIFIER = StringUtils.EMPTY;

  public static final Gson GENERICS_AWARE_GSON = GsonInterfaceAdapter.getGson(Object.class);
  public static final Splitter COMMA_BASED_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

  protected MetricContext metricContext;
  protected EventSubmitter eventSubmitter;
  protected AvroSchemaManager avroSchemaManager;

  protected HiveUnitUpdateProvider updateProvider;
  protected HiveSourceWatermarker watermarker;
  protected IterableDatasetFinder<HiveDataset> datasetFinder;
  protected List<WorkUnit> workunits;
  protected long maxLookBackTime;
  protected long beginGetWorkunitsTime;
  protected List<String> ignoreDataPathIdentifierList;

  protected final ClassAliasResolver<HiveBaseExtractorFactory> classAliasResolver =
      new ClassAliasResolver<>(HiveBaseExtractorFactory.class);

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
          if (HiveUtils.isPartitioned(hiveDataset.getTable())
              && state.getPropAsBoolean(HIVE_SOURCE_CREATE_WORKUNITS_FOR_PARTITIONS,
              DEFAULT_HIVE_SOURCE_CREATE_WORKUNITS_FOR_PARTITIONS)) {
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
    this.avroSchemaManager = new AvroSchemaManager(getSourceFs(state), state);
    this.workunits = Lists.newArrayList();

    this.watermarker =
        GobblinConstructorUtils.invokeConstructor(HiveSourceWatermarkerFactory.class,
            state.getProp(HIVE_SOURCE_WATERMARKER_FACTORY_CLASS_KEY, DEFAULT_HIVE_SOURCE_WATERMARKER_FACTORY_CLASS))
            .createFromState(state);

    EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.CONVERSION_SETUP_EVENT);
    this.datasetFinder = GobblinConstructorUtils.invokeConstructor(HiveDatasetFinder.class,
        state.getProp(HIVE_SOURCE_DATASET_FINDER_CLASS_KEY, DEFAULT_HIVE_SOURCE_DATASET_FINDER_CLASS), getSourceFs(state), state.getProperties(),
        this.eventSubmitter);
    int maxLookBackDays = state.getPropAsInt(HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS);
    this.maxLookBackTime = new DateTime().minusDays(maxLookBackDays).getMillis();
    this.ignoreDataPathIdentifierList = COMMA_BASED_SPLITTER.splitToList(state.getProp(HIVE_SOURCE_IGNORE_DATA_PATH_IDENTIFIER_KEY,
        DEFAULT_HIVE_SOURCE_IGNORE_DATA_PATH_IDENTIFIER));

    silenceHiveLoggers();
  }


  protected void createWorkunitForNonPartitionedTable(HiveDataset hiveDataset) throws IOException {
    // Create workunits for tables
    try {

      long tableProcessTime = new DateTime().getMillis();

      long updateTime = this.updateProvider.getUpdateTime(hiveDataset.getTable());

      this.watermarker.onTableProcessBegin(hiveDataset.getTable(), tableProcessTime);

      LongWatermark lowWatermark = this.watermarker.getPreviousHighWatermark(hiveDataset.getTable());

      if (!shouldCreateWorkUnit(hiveDataset.getTable().getPath())) {
        log.info(String.format(
            "Not creating workunit for table %s as partition path %s contains data path tokens to ignore %s",
            hiveDataset.getTable().getCompleteName(), hiveDataset.getTable().getPath(), this.ignoreDataPathIdentifierList));
        return;
      }

      if (shouldCreateWorkunit(hiveDataset.getTable(), lowWatermark)) {

        log.info(String.format(
            "Creating workunit for table %s as updateTime %s or createTime %s is greater than low watermark %s",
            hiveDataset.getTable().getCompleteName(), updateTime, hiveDataset.getTable().getTTable().getCreateTime(),
            lowWatermark.getValue()));
        HiveWorkUnit hiveWorkUnit = workUnitForTable(hiveDataset);

        LongWatermark expectedDatasetHighWatermark =
            this.watermarker.getExpectedHighWatermark(hiveDataset.getTable(), tableProcessTime);
        hiveWorkUnit.setWatermarkInterval(new WatermarkInterval(lowWatermark, expectedDatasetHighWatermark));

        EventWorkunitUtils.setTableSlaEventMetadata(hiveWorkUnit, hiveDataset.getTable(), updateTime, lowWatermark.getValue(),
            this.beginGetWorkunitsTime);

        this.workunits.add(hiveWorkUnit);
        log.debug(String.format("Workunit added for table: %s", hiveWorkUnit));

      } else {
        log.info(String
            .format(
                "Not creating workunit for table %s as updateTime %s and createTime %s is not greater than low watermark %s",
                hiveDataset.getTable().getCompleteName(), updateTime, hiveDataset.getTable().getTTable()
                    .getCreateTime(), lowWatermark.getValue()));
      }
    } catch (UpdateNotFoundException e) {
      log.error(String.format("Not Creating workunit for %s as update time was not found. %s", hiveDataset.getTable()
          .getCompleteName(), e.getMessage()), e);
    } catch (SchemaNotFoundException e) {
      log.error(String.format("Not Creating workunit for %s as schema was not found. %s", hiveDataset.getTable()
          .getCompleteName(), e.getMessage()), e);
    }
  }

  protected HiveWorkUnit workUnitForTable(HiveDataset hiveDataset) throws IOException {
    HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(hiveDataset);
    if (isAvro(hiveDataset.getTable())) {
      hiveWorkUnit.setTableSchemaUrl(this.avroSchemaManager.getSchemaUrl(hiveDataset.getTable()));
    }
    return hiveWorkUnit;
  }

  protected void createWorkunitsForPartitionedTable(HiveDataset hiveDataset, AutoReturnableObject<IMetaStoreClient> client) throws IOException {

    long tableProcessTime = new DateTime().getMillis();
    this.watermarker.onTableProcessBegin(hiveDataset.getTable(), tableProcessTime);

    Optional<String> partitionFilter = Optional.absent();

    // If the table is date partitioned, use the partition name to filter partitions older than lookback
    if (hiveDataset.getProperties().containsKey(LookbackPartitionFilterGenerator.PARTITION_COLUMN)
        && hiveDataset.getProperties().containsKey(LookbackPartitionFilterGenerator.DATETIME_FORMAT)
        && hiveDataset.getProperties().containsKey(LookbackPartitionFilterGenerator.LOOKBACK)) {
      partitionFilter =
          Optional.of(new LookbackPartitionFilterGenerator(hiveDataset.getProperties()).getFilter(hiveDataset));
      log.info(String.format("Getting partitions for %s using partition filter %s", hiveDataset.getTable()
          .getCompleteName(), partitionFilter.get()));
    }

    List<Partition> sourcePartitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), partitionFilter);

    for (Partition sourcePartition : sourcePartitions) {
      if (isOlderThanLookback(sourcePartition)) {
        continue;
      }

      LongWatermark lowWatermark = watermarker.getPreviousHighWatermark(sourcePartition);

      try {
        if (!shouldCreateWorkUnit(new Path(sourcePartition.getLocation()))) {
          log.info(String.format(
              "Not creating workunit for partition %s as partition path %s contains data path tokens to ignore %s",
              sourcePartition.getCompleteName(), sourcePartition.getLocation(), this.ignoreDataPathIdentifierList));
          continue;
        }

        long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
        if (shouldCreateWorkunit(sourcePartition, lowWatermark)) {
          log.debug(String.format("Processing partition: %s", sourcePartition));

          long partitionProcessTime = new DateTime().getMillis();
          this.watermarker.onPartitionProcessBegin(sourcePartition, partitionProcessTime, updateTime);

          LongWatermark expectedPartitionHighWatermark = this.watermarker.getExpectedHighWatermark(sourcePartition,
              tableProcessTime, partitionProcessTime);

          HiveWorkUnit hiveWorkUnit = workUnitForPartition(hiveDataset, sourcePartition);
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
        log.error(String.format("Not creating workunit for %s as update time was not found. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      } catch (SchemaNotFoundException e) {
        log.error(String.format("Not creating workunit for %s as schema was not found. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      } catch (UncheckedExecutionException e) {
        log.error(String.format("Not creating workunit for %s because an unchecked exception occurred. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      }
    }
  }

  protected HiveWorkUnit workUnitForPartition(HiveDataset hiveDataset, Partition partition) throws IOException {
    HiveWorkUnit hiveWorkUnit = new HiveWorkUnit(hiveDataset, partition);
    if (isAvro(hiveDataset.getTable())) {
      hiveWorkUnit.setTableSchemaUrl(this.avroSchemaManager.getSchemaUrl(hiveDataset.getTable()));
      hiveWorkUnit.setPartitionSchemaUrl(this.avroSchemaManager.getSchemaUrl(partition));
    }
    return hiveWorkUnit;
  }

  /***
   * Check if path of Hive entity (table / partition) contains location token that should be ignored. If so, ignore
   * the partition.
   */
  protected boolean shouldCreateWorkUnit(Path dataLocation) {
    if (null == this.ignoreDataPathIdentifierList || this.ignoreDataPathIdentifierList.size() == 0) {
      return true;
    }
    for (String pathToken : this.ignoreDataPathIdentifierList) {
      if (dataLocation.toString().toLowerCase().contains(pathToken.toLowerCase())) {
        return false;
      }
    }

    return true;
  }

  protected boolean shouldCreateWorkunit(Partition sourcePartition, LongWatermark lowWatermark) throws UpdateNotFoundException {
    long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
    long createTime = getCreateTime(sourcePartition);
    return shouldCreateWorkunit(createTime, updateTime, lowWatermark);
  }

  protected boolean shouldCreateWorkunit(Table table, LongWatermark lowWatermark)
      throws UpdateNotFoundException {
    long updateTime = this.updateProvider.getUpdateTime(table);
    long createTime = getCreateTime(table);
    return shouldCreateWorkunit(createTime, updateTime, lowWatermark);
  }

  /**
   * Check if workunit needs to be created. Returns <code>true</code> If the
   * <code>updateTime</code> is greater than the <code>lowWatermark</code> and <code>maxLookBackTime</code>
   * <code>createTime</code> is not used. It exists for backward compatibility
   */
  protected boolean shouldCreateWorkunit(long createTime, long updateTime, LongWatermark lowWatermark) {
    if (new DateTime(updateTime).isBefore(this.maxLookBackTime)) {
      return false;
    }
    return new DateTime(updateTime).isAfter(lowWatermark.getValue());
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
    else if (partition.getTPartition().isSetParameters()
        && partition.getTPartition().getParameters().containsKey(DISTCP_REGISTRATION_GENERATION_TIME_KEY)) {
      log.debug("Did not find createTime in Hive partition, used distcp registration generation time.");
      return Long.parseLong(partition.getTPartition().getParameters().get(DISTCP_REGISTRATION_GENERATION_TIME_KEY));
    } else {
      log.warn(String.format("Could not find create time for partition %s. Will return createTime as 0",
          partition.getCompleteName()));
      return 0;
    }
  }

  // Convert createTime from seconds to milliseconds
  protected static long getCreateTime(Table table) {
    return TimeUnit.MILLISECONDS.convert(table.getTTable().getCreateTime(), TimeUnit.SECONDS);
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) throws IOException {
    try {
      return classAliasResolver.resolveClass(state.getProp(HIVE_SOURCE_EXTRACTOR_TYPE, DEFAULT_HIVE_SOURCE_EXTRACTOR_TYPE))
          .newInstance().createExtractor(state, getSourceFs(state));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void shutdown(SourceState state) {
  }

  public static FileSystem getSourceFs(State state) throws IOException {
    if (state.contains(HIVE_SOURCE_FS_URI)) {
      return FileSystem.get(URI.create(state.getProp(HIVE_SOURCE_FS_URI)), HadoopUtils.getConfFromState(state));
    }
    return FileSystem.get(HadoopUtils.getConfFromState(state));
  }

  /**
   * Hive logging is too verbose at INFO level. Currently hive does not have a way to set log level.
   * This is a workaround to set log level to WARN for hive loggers only
   */
  private void silenceHiveLoggers() {
    List<String> loggers = ImmutableList.of("org.apache.hadoop.hive", "org.apache.hive", "hive.ql.parse");
    for (String name : loggers) {
      Logger logger = Logger.getLogger(name);
      if (logger != null) {
        logger.setLevel(Level.WARN);
      }
    }
  }

  private boolean isAvro(Table table) {
    return AvroSerDe.class.getName().equals(table.getSd().getSerdeInfo().getSerializationLib());
  }
}
