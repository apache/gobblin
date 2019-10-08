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

package org.apache.gobblin.data.management.copy;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.extractor.EmptyExtractor;
import org.apache.gobblin.data.management.copy.extractor.FileAwareInputStreamExtractor;
import org.apache.gobblin.data.management.copy.prioritization.FileSetComparator;
import org.apache.gobblin.data.management.copy.publisher.CopyEventSubmitterHelper;
import org.apache.gobblin.data.management.copy.replication.ConfigBasedDataset;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.data.management.copy.watermark.CopyableFileWatermarkGenerator;
import org.apache.gobblin.data.management.copy.watermark.CopyableFileWatermarkHelper;
import org.apache.gobblin.data.management.dataset.DatasetUtils;
import org.apache.gobblin.data.management.partition.CopyableDatasetRequestor;
import org.apache.gobblin.data.management.partition.FileSet;
import org.apache.gobblin.data.management.partition.FileSetResourceEstimator;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.gobblin.dataset.IterableDatasetFinderImpl;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.metrics.event.sla.SlaEventKeys;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.WatermarkInterval;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.source.workunit.WorkUnitWeighter;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.binpacking.FieldWeighter;
import org.apache.gobblin.util.binpacking.WorstFitDecreasingBinPacking;
import org.apache.gobblin.util.deprecation.DeprecationUtils;
import org.apache.gobblin.util.executors.IteratorExecutor;
import org.apache.gobblin.util.guid.Guid;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.util.request_allocation.GreedyAllocator;
import org.apache.gobblin.util.request_allocation.HierarchicalAllocator;
import org.apache.gobblin.util.request_allocation.HierarchicalPrioritizer;
import org.apache.gobblin.util.request_allocation.PriorityIterableBasedRequestAllocator;
import org.apache.gobblin.util.request_allocation.RequestAllocator;
import org.apache.gobblin.util.request_allocation.RequestAllocatorConfig;
import org.apache.gobblin.util.request_allocation.RequestAllocatorUtils;


/**
 * {@link org.apache.gobblin.source.Source} that generates work units from {@link org.apache.gobblin.data.management.copy.CopyableDataset}s.
 *
 */
@Slf4j
public class CopySource extends AbstractSource<String, FileAwareInputStream> {

  public static final String DEFAULT_DATASET_PROFILE_CLASS_KEY = CopyableGlobDatasetFinder.class.getCanonicalName();
  public static final String SERIALIZED_COPYABLE_FILE = CopyConfiguration.COPY_PREFIX + ".serialized.copyable.file";
  public static final String COPY_ENTITY_CLASS = CopyConfiguration.COPY_PREFIX + ".copy.entity.class";
  public static final String SERIALIZED_COPYABLE_DATASET =
      CopyConfiguration.COPY_PREFIX + ".serialized.copyable.datasets";
  public static final String WORK_UNIT_GUID = CopyConfiguration.COPY_PREFIX + ".work.unit.guid";
  public static final String MAX_CONCURRENT_LISTING_SERVICES =
      CopyConfiguration.COPY_PREFIX + ".max.concurrent.listing.services";
  public static final int DEFAULT_MAX_CONCURRENT_LISTING_SERVICES = 20;
  public static final String MAX_FILES_COPIED_KEY = CopyConfiguration.COPY_PREFIX + ".max.files.copied";
  public static final String SIMULATE = CopyConfiguration.COPY_PREFIX + ".simulate";
  public static final String MAX_SIZE_MULTI_WORKUNITS = CopyConfiguration.COPY_PREFIX + ".binPacking.maxSizePerBin";
  public static final String MAX_WORK_UNITS_PER_BIN = CopyConfiguration.COPY_PREFIX + ".binPacking.maxWorkUnitsPerBin";
  public static final String REQUESTS_EXCEEDING_AVAILABLE_RESOURCE_POOL_EVENT_NAME =
      "RequestsExceedingAvailableResourcePoolEvent";
  public static final String REQUESTS_DROPPED_EVENT_NAME = "RequestsDroppedEvent";
  public static final String REQUESTS_REJECTED_DUE_TO_INSUFFICIENT_EVICTION_EVENT_NAME =
      "RequestsRejectedDueToInsufficientEvictionEvent";
  public static final String REQUESTS_REJECTED_WITH_LOW_PRIORITY_EVENT_NAME = "RequestsRejectedWithLowPriorityEvent";
  public static final String FILESET_NAME = "fileset.name";
  public static final String FILESET_TOTAL_ENTITIES = "fileset.total.entities";
  public static final String FILESET_TOTAL_SIZE_IN_BYTES = "fileset.total.size";
  public static final String SCHEMA_CHECK_ENABLED = "shcema.check.enabled";
  public final static boolean DEFAULT_SCHEMA_CHECK_ENABLED = false;

  private static final String WORK_UNIT_WEIGHT = CopyConfiguration.COPY_PREFIX + ".workUnitWeight";
  private final WorkUnitWeighter weighter = new FieldWeighter(WORK_UNIT_WEIGHT);

  public MetricContext metricContext;
  public EventSubmitter eventSubmitter;

  protected Optional<LineageInfo> lineageInfo;

  /**
   * <ul>
   * Does the following:
   * <li>Instantiate a {@link DatasetsFinder}.
   * <li>Find all {@link Dataset} using {@link DatasetsFinder}.
   * <li>For each {@link CopyableDataset} get all {@link CopyEntity}s.
   * <li>Create a {@link WorkUnit} per {@link CopyEntity}.
   * </ul>
   *
   * <p>
   * In this implementation, one workunit is created for every {@link CopyEntity} found. But the extractor/converters
   * and writers are built to support multiple {@link CopyEntity}s per workunit
   * </p>
   *
   * @param state see {@link org.apache.gobblin.configuration.SourceState}
   * @return Work units for copying files.
   */
  @Override
  public List<WorkUnit> getWorkunits(final SourceState state) {

    this.metricContext = Instrumented.getMetricContext(state, CopySource.class);
    this.lineageInfo = LineageInfo.getLineageInfo(state.getBroker());

    try {

      DeprecationUtils
          .renameDeprecatedKeys(state, CopyConfiguration.MAX_COPY_PREFIX + "." + CopyResourcePool.ENTITIES_KEY,
              Lists.newArrayList(MAX_FILES_COPIED_KEY));

      final FileSystem sourceFs = HadoopUtils.getSourceFileSystem(state);
      final FileSystem targetFs = HadoopUtils.getWriterFileSystem(state, 1, 0);
      state.setProp(SlaEventKeys.SOURCE_URI, sourceFs.getUri());
      state.setProp(SlaEventKeys.DESTINATION_URI, targetFs.getUri());

      log.info("Identified source file system at {} and target file system at {}.", sourceFs.getUri(),
          targetFs.getUri());

      long maxSizePerBin = state.getPropAsLong(MAX_SIZE_MULTI_WORKUNITS, 0);
      long maxWorkUnitsPerMultiWorkUnit = state.getPropAsLong(MAX_WORK_UNITS_PER_BIN, 50);
      final long minWorkUnitWeight = Math.max(1, maxSizePerBin / maxWorkUnitsPerMultiWorkUnit);
      final Optional<CopyableFileWatermarkGenerator> watermarkGenerator =
          CopyableFileWatermarkHelper.getCopyableFileWatermarkGenerator(state);
      int maxThreads = state.getPropAsInt(MAX_CONCURRENT_LISTING_SERVICES, DEFAULT_MAX_CONCURRENT_LISTING_SERVICES);

      final CopyConfiguration copyConfiguration = CopyConfiguration.builder(targetFs, state.getProperties()).build();

      this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, CopyConfiguration.COPY_PREFIX).build();
      DatasetsFinder<CopyableDatasetBase> datasetFinder = DatasetUtils
          .instantiateDatasetFinder(state.getProperties(), sourceFs, DEFAULT_DATASET_PROFILE_CLASS_KEY,
              this.eventSubmitter, state);

      IterableDatasetFinder<CopyableDatasetBase> iterableDatasetFinder =
          datasetFinder instanceof IterableDatasetFinder ? (IterableDatasetFinder<CopyableDatasetBase>) datasetFinder
              : new IterableDatasetFinderImpl<>(datasetFinder);

      Iterator<CopyableDatasetRequestor> requestorIteratorWithNulls = Iterators
          .transform(iterableDatasetFinder.getDatasetsIterator(),
              new CopyableDatasetRequestor.Factory(targetFs, copyConfiguration, log));
      Iterator<CopyableDatasetRequestor> requestorIterator =
          Iterators.filter(requestorIteratorWithNulls, Predicates.<CopyableDatasetRequestor>notNull());

      final SetMultimap<FileSet<CopyEntity>, WorkUnit> workUnitsMap =
          Multimaps.<FileSet<CopyEntity>, WorkUnit>synchronizedSetMultimap(
              HashMultimap.<FileSet<CopyEntity>, WorkUnit>create());

      RequestAllocator<FileSet<CopyEntity>> allocator = createRequestAllocator(copyConfiguration, maxThreads);
      Iterator<FileSet<CopyEntity>> prioritizedFileSets =
          allocator.allocateRequests(requestorIterator, copyConfiguration.getMaxToCopy());

      //Submit alertable events for unfulfilled requests
      submitUnfulfilledRequestEvents(allocator);

      String filesetWuGeneratorAlias = state.getProp(ConfigurationKeys.COPY_SOURCE_FILESET_WU_GENERATOR_CLASS, FileSetWorkUnitGenerator.class.getName());
      Iterator<Callable<Void>> callableIterator =
          Iterators.transform(prioritizedFileSets, new Function<FileSet<CopyEntity>, Callable<Void>>() {
            @Nullable
            @Override
            public Callable<Void> apply(FileSet<CopyEntity> input) {
              try {
                return GobblinConstructorUtils.<FileSetWorkUnitGenerator>invokeLongestConstructor(
                    new ClassAliasResolver(FileSetWorkUnitGenerator.class).resolveClass(filesetWuGeneratorAlias),
                    input.getDataset(), input, state, targetFs, workUnitsMap, watermarkGenerator, minWorkUnitWeight, lineageInfo);
              } catch (Exception e) {
                throw new RuntimeException("Cannot create workunits generator", e);
              }
            }
          });

      try {
        List<Future<Void>> futures = new IteratorExecutor<>(callableIterator, maxThreads,
            ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("Copy-file-listing-pool-%d")))
            .execute();

        for (Future<Void> future : futures) {
          try {
            future.get();
          } catch (ExecutionException exc) {
            log.error("Failed to get work units for dataset.", exc.getCause());
          }
        }
      } catch (InterruptedException ie) {
        log.error("Retrieval of work units was interrupted. Aborting.");
        return Lists.newArrayList();
      }

      log.info(String.format("Created %s workunits ", workUnitsMap.size()));

      copyConfiguration.getCopyContext().logCacheStatistics();

      if (state.contains(SIMULATE) && state.getPropAsBoolean(SIMULATE)) {
        log.info("Simulate mode enabled. Will not execute the copy.");
        for (Map.Entry<FileSet<CopyEntity>, Collection<WorkUnit>> entry : workUnitsMap.asMap().entrySet()) {
          log.info(String.format("Actions for dataset %s file set %s.", entry.getKey().getDataset().datasetURN(),
              entry.getKey().getName()));
          for (WorkUnit workUnit : entry.getValue()) {
            try {
              CopyEntity copyEntity = deserializeCopyEntity(workUnit);
              log.info(copyEntity.explain());
            } catch (Exception e) {
              log.info("Cannot deserialize CopyEntity from wu : {}", workUnit.toString());
            }
          }
        }
        return Lists.newArrayList();
      }

      List<? extends WorkUnit> workUnits = new WorstFitDecreasingBinPacking(maxSizePerBin)
          .pack(Lists.newArrayList(workUnitsMap.values()), this.weighter);
      log.info(String.format(
          "Bin packed work units. Initial work units: %d, packed work units: %d, max weight per bin: %d, "
              + "max work units per bin: %d.", workUnitsMap.size(), workUnits.size(), maxSizePerBin,
          maxWorkUnitsPerMultiWorkUnit));
      return ImmutableList.copyOf(workUnits);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void submitUnfulfilledRequestEventsHelper(List<FileSet<CopyEntity>> fileSetList, String eventName) {
    for (FileSet<CopyEntity> fileSet : fileSetList) {
      GobblinTrackingEvent event =
          GobblinTrackingEvent.newBuilder().setName(eventName).setNamespace(CopySource.class.getName()).setMetadata(
              ImmutableMap.<String, String>builder()
                  .put(ConfigurationKeys.DATASET_URN_KEY, fileSet.getDataset().getUrn())
                  .put(FILESET_TOTAL_ENTITIES, Integer.toString(fileSet.getTotalEntities()))
                  .put(FILESET_TOTAL_SIZE_IN_BYTES, Long.toString(fileSet.getTotalSizeInBytes()))
                  .put(FILESET_NAME, fileSet.getName()).build()).build();
      this.metricContext.submitEvent(event);
    }
  }

  private void submitUnfulfilledRequestEvents(RequestAllocator<FileSet<CopyEntity>> allocator) {
    if (PriorityIterableBasedRequestAllocator.class.isAssignableFrom(allocator.getClass())) {
      PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>> priorityIterableBasedRequestAllocator =
          (PriorityIterableBasedRequestAllocator<FileSet<CopyEntity>>) allocator;
      submitUnfulfilledRequestEventsHelper(
          priorityIterableBasedRequestAllocator.getRequestsExceedingAvailableResourcePool(),
          REQUESTS_EXCEEDING_AVAILABLE_RESOURCE_POOL_EVENT_NAME);
      submitUnfulfilledRequestEventsHelper(
          priorityIterableBasedRequestAllocator.getRequestsRejectedDueToInsufficientEviction(),
          REQUESTS_REJECTED_DUE_TO_INSUFFICIENT_EVICTION_EVENT_NAME);
      submitUnfulfilledRequestEventsHelper(priorityIterableBasedRequestAllocator.getRequestsRejectedWithLowPriority(),
          REQUESTS_REJECTED_WITH_LOW_PRIORITY_EVENT_NAME);
      submitUnfulfilledRequestEventsHelper(priorityIterableBasedRequestAllocator.getRequestsDropped(),
          REQUESTS_DROPPED_EVENT_NAME);
    }
  }

  private RequestAllocator<FileSet<CopyEntity>> createRequestAllocator(CopyConfiguration copyConfiguration,
      int maxThreads) {
    Optional<FileSetComparator> prioritizer = copyConfiguration.getPrioritizer();
    RequestAllocatorConfig.Builder<FileSet<CopyEntity>> configBuilder =
        RequestAllocatorConfig.builder(new FileSetResourceEstimator()).allowParallelization(maxThreads)
            .storeRejectedRequests(copyConfiguration.getStoreRejectedRequestsSetting())
            .withLimitedScopeConfig(copyConfiguration.getPrioritizationConfig());

    if (!prioritizer.isPresent()) {
      return new GreedyAllocator<>(configBuilder.build());
    } else {
      configBuilder.withPrioritizer(prioritizer.get());
    }

    if (prioritizer.get() instanceof HierarchicalPrioritizer) {
      return new HierarchicalAllocator.Factory().createRequestAllocator(configBuilder.build());
    } else {
      return RequestAllocatorUtils.inferFromConfig(configBuilder.build());
    }
  }

  /**
   * {@link Runnable} to generate copy listing for one {@link CopyableDataset}.
   */
  @Alias("FileSetWorkUnitGenerator")
  @AllArgsConstructor
  public static class FileSetWorkUnitGenerator implements Callable<Void> {

    protected final CopyableDatasetBase copyableDataset;
    protected final FileSet<CopyEntity> fileSet;
    protected final State state;
    protected final FileSystem targetFs;
    protected final SetMultimap<FileSet<CopyEntity>, WorkUnit> workUnitList;
    protected final Optional<CopyableFileWatermarkGenerator> watermarkGenerator;
    protected final long minWorkUnitWeight;
    protected final Optional<LineageInfo> lineageInfo;

    @Override
    public Void call() {

      try {
        String extractId = fileSet.getName().replace(':', '_');

        Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, CopyConfiguration.COPY_PREFIX, extractId);
        List<WorkUnit> workUnitsForPartition = Lists.newArrayList();
        for (CopyEntity copyEntity : fileSet.getFiles()) {

          CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(this.copyableDataset);
          CopyEntity.DatasetAndPartition datasetAndPartition = copyEntity.getDatasetAndPartition(metadata);

          WorkUnit workUnit = new WorkUnit(extract);
          workUnit.addAll(this.state);
          if(this.copyableDataset instanceof ConfigBasedDataset && ((ConfigBasedDataset)this.copyableDataset).schemaCheckEnabled()) {
            workUnit.setProp(SCHEMA_CHECK_ENABLED, true);
            if (((ConfigBasedDataset) this.copyableDataset).getExpectedSchema() != null) {
              workUnit.setProp(ConfigurationKeys.COPY_EXPECTED_SCHEMA, ((ConfigBasedDataset) this.copyableDataset).getExpectedSchema());
            }
          }
          serializeCopyEntity(workUnit, copyEntity);
          serializeCopyableDataset(workUnit, metadata);
          GobblinMetrics.addCustomTagToState(workUnit,
              new Tag<>(CopyEventSubmitterHelper.DATASET_ROOT_METADATA_NAME, this.copyableDataset.datasetURN()));
          workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetAndPartition.toString());
          workUnit.setProp(SlaEventKeys.DATASET_URN_KEY, this.copyableDataset.datasetURN());
          workUnit.setProp(SlaEventKeys.PARTITION_KEY, copyEntity.getFileSet());
          setWorkUnitWeight(workUnit, copyEntity, minWorkUnitWeight);
          setWorkUnitWatermark(workUnit, watermarkGenerator, copyEntity);
          computeAndSetWorkUnitGuid(workUnit);
          addLineageInfo(copyEntity, workUnit);
          if (copyEntity instanceof CopyableFile && DistcpFileSplitter.allowSplit(this.state, this.targetFs)) {
            workUnitsForPartition.addAll(DistcpFileSplitter.splitFile((CopyableFile) copyEntity, workUnit, this.targetFs));
          } else {
            workUnitsForPartition.add(workUnit);
          }
        }

        this.workUnitList.putAll(this.fileSet, workUnitsForPartition);

        return null;
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to generate work units for dataset " + this.copyableDataset.datasetURN(),
            ioe);
      }
    }

    private void setWorkUnitWatermark(WorkUnit workUnit, Optional<CopyableFileWatermarkGenerator> watermarkGenerator,
        CopyEntity copyEntity)
        throws IOException {
      if (copyEntity instanceof CopyableFile) {
        Optional<WatermarkInterval> watermarkIntervalOptional =
            CopyableFileWatermarkHelper.getCopyableFileWatermark((CopyableFile) copyEntity, watermarkGenerator);
        if (watermarkIntervalOptional.isPresent()) {
          workUnit.setWatermarkInterval(watermarkIntervalOptional.get());
        }
      }
    }

    private void addLineageInfo(CopyEntity copyEntity, WorkUnit workUnit) {
      if (copyEntity instanceof CopyableFile) {
        CopyableFile copyableFile = (CopyableFile) copyEntity;
      /*
       * In Gobblin Distcp, the source and target path info of a CopyableFile are determined by its dataset found by
       * a DatasetFinder. Consequently, the source and destination dataset for the CopyableFile lineage are expected
       * to be set by the same logic
       */
        if (lineageInfo.isPresent() && copyableFile.getSourceData() != null
            && copyableFile.getDestinationData() != null) {
          lineageInfo.get().setSource(copyableFile.getSourceData(), workUnit);
        }
      }
    }
  }

  /**
   * @param state a {@link org.apache.gobblin.configuration.WorkUnitState} carrying properties needed by the returned
   *          {@link Extractor}
   * @return a {@link FileAwareInputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state)
      throws IOException {

    Class<?> copyEntityClass = getCopyEntityClass(state);

    if (CopyableFile.class.isAssignableFrom(copyEntityClass)) {
      CopyableFile copyEntity = (CopyableFile) deserializeCopyEntity(state);
      return extractorForCopyableFile(HadoopUtils.getSourceFileSystem(state), copyEntity, state);
    }
    return new EmptyExtractor<>("empty");
  }

  protected Extractor<String, FileAwareInputStream> extractorForCopyableFile(FileSystem fs, CopyableFile cf,
      WorkUnitState state)
      throws IOException {
    return new FileAwareInputStreamExtractor(fs, cf, state);
  }

  @Override
  public void shutdown(SourceState state) {
  }

  /**
   * @deprecated use {@link HadoopUtils#getSourceFileSystem(State)}.
   */
  @Deprecated
  protected FileSystem getSourceFileSystem(State state)
      throws IOException {
    Configuration conf =
        HadoopUtils.getConfFromState(state, Optional.of(ConfigurationKeys.SOURCE_FILEBASED_ENCRYPTED_CONFIG_PATH));
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  /**
   * @deprecated use {@link HadoopUtils#getWriterFileSystem(State, int, int)}.
   */
  @Deprecated
  private static FileSystem getTargetFileSystem(State state)
      throws IOException {
    return HadoopUtils.getOptionallyThrottledFileSystem(WriterUtils.getWriterFS(state, 1, 0), state);
  }

  private static void setWorkUnitWeight(WorkUnit workUnit, CopyEntity copyEntity, long minWeight) {
    long weight = 0;
    if (copyEntity instanceof CopyableFile) {
      weight = ((CopyableFile) copyEntity).getOrigin().getLen();
    }
    weight = Math.max(weight, minWeight);
    workUnit.setProp(WORK_UNIT_WEIGHT, Long.toString(weight));
  }

  private static void computeAndSetWorkUnitGuid(WorkUnit workUnit)
      throws IOException {
    Guid guid = Guid.fromStrings(workUnit.contains(ConfigurationKeys.CONVERTER_CLASSES_KEY) ? workUnit
        .getProp(ConfigurationKeys.CONVERTER_CLASSES_KEY) : "");
    setWorkUnitGuid(workUnit, guid.append(deserializeCopyEntity(workUnit)));
  }

  /**
   * Set a unique, replicable guid for this work unit. Used for recovering partially successful work units.
   * @param state {@link State} where guid should be written.
   * @param guid A byte array guid.
   */
  public static void setWorkUnitGuid(State state, Guid guid) {
    state.setProp(WORK_UNIT_GUID, guid.toString());
  }

  /**
   * Get guid in this state if available. This is the reverse operation of {@link #setWorkUnitGuid}.
   * @param state State from which guid should be extracted.
   * @return A byte array guid.
   * @throws IOException
   */
  public static Optional<Guid> getWorkUnitGuid(State state)
      throws IOException {
    if (state.contains(WORK_UNIT_GUID)) {
      return Optional.of(Guid.deserialize(state.getProp(WORK_UNIT_GUID)));
    }
    return Optional.absent();
  }

  /**
   * Serialize a {@link List} of {@link CopyEntity}s into a {@link State} at {@link #SERIALIZED_COPYABLE_FILE}
   */
  public static void serializeCopyEntity(State state, CopyEntity copyEntity) {
    state.setProp(SERIALIZED_COPYABLE_FILE, CopyEntity.serialize(copyEntity));
    state.setProp(COPY_ENTITY_CLASS, copyEntity.getClass().getName());
  }

  public static Class<?> getCopyEntityClass(State state)
      throws IOException {
    try {
      return Class.forName(state.getProp(COPY_ENTITY_CLASS));
    } catch (ClassNotFoundException cnfe) {
      throw new IOException(cnfe);
    }
  }

  /**
   * Deserialize a {@link List} of {@link CopyEntity}s from a {@link State} at {@link #SERIALIZED_COPYABLE_FILE}
   */
  public static CopyEntity deserializeCopyEntity(State state) {
    return CopyEntity.deserialize(state.getProp(SERIALIZED_COPYABLE_FILE));
  }

  /**
   * Serialize a {@link CopyableDataset} into a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static void serializeCopyableDataset(State state, CopyableDatasetMetadata copyableDataset) {
    state.setProp(SERIALIZED_COPYABLE_DATASET, copyableDataset.serialize());
  }

  /**
   * Deserialize a {@link CopyableDataset} from a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static CopyableDatasetMetadata deserializeCopyableDataset(State state) {
    return CopyableDatasetMetadata.deserialize(state.getProp(SERIALIZED_COPYABLE_DATASET));
  }
}
