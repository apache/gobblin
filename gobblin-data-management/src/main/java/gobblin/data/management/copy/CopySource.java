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

package gobblin.data.management.copy;

import java.io.IOException;
import java.net.URI;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.extractor.EmptyExtractor;
import gobblin.data.management.copy.extractor.FileAwareInputStreamExtractor;
import gobblin.data.management.copy.publisher.CopyEventSubmitterHelper;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.FileSet;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.dataset.IterableDatasetFinderImpl;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;
import gobblin.util.binpacking.FieldWeighter;
import gobblin.source.workunit.WorkUnitWeighter;
import gobblin.util.binpacking.WorstFitDecreasingBinPacking;
import gobblin.util.executors.IteratorExecutor;
import gobblin.util.guid.Guid;
import gobblin.util.iterators.InterruptibleIterator;


/**
 * {@link gobblin.source.Source} that generates work units from {@link gobblin.data.management.copy.CopyableDataset}s.
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
  public static final int DEFAULT_MAX_FILES_COPIED = 100000;
  public static final String SIMULATE = CopyConfiguration.COPY_PREFIX + ".simulate";
  public static final String MAX_SIZE_MULTI_WORKUNITS = CopyConfiguration.COPY_PREFIX + ".binPacking.maxSizePerBin";
  public static final String MAX_WORK_UNITS_PER_BIN = CopyConfiguration.COPY_PREFIX + ".binPacking.maxWorkUnitsPerBin";

  private static final String WORK_UNIT_WEIGHT = CopyConfiguration.COPY_PREFIX + ".workUnitWeight";

  private final WorkUnitWeighter weighter = new FieldWeighter(WORK_UNIT_WEIGHT);

  public MetricContext metricContext;

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
   * @param state see {@link gobblin.configuration.SourceState}
   * @return Work units for copying files.
   */
  @Override
  public List<WorkUnit> getWorkunits(final SourceState state) {
    this.metricContext = Instrumented.getMetricContext(state, CopySource.class);

    try {

      final FileSystem sourceFs = getSourceFileSystem(state);
      final FileSystem targetFs = getTargetFileSystem(state);
      long maxSizePerBin = state.getPropAsLong(MAX_SIZE_MULTI_WORKUNITS, 0);
      long maxWorkUnitsPerMultiWorkUnit = state.getPropAsLong(MAX_WORK_UNITS_PER_BIN, 50);
      final long minWorkUnitWeight = Math.max(1, maxSizePerBin / maxWorkUnitsPerMultiWorkUnit);

      // TODO: The comparator sets the priority of file sets. Currently, all file sets have the same priority, this needs to
      // be pluggable.
      final ConcurrentBoundedWorkUnitList workUnitList = ConcurrentBoundedWorkUnitList.builder()
          .maxSize(state.getPropAsInt(MAX_FILES_COPIED_KEY, DEFAULT_MAX_FILES_COPIED)).strictLimitMultiplier(2).build();

      final CopyConfiguration copyConfiguration = CopyConfiguration.builder(targetFs, state.getProperties()).build();

      DatasetsFinder<CopyableDatasetBase> datasetFinder =
          DatasetUtils.instantiateDatasetFinder(state.getProperties(), sourceFs, DEFAULT_DATASET_PROFILE_CLASS_KEY,
              new EventSubmitter.Builder(this.metricContext, CopyConfiguration.COPY_PREFIX).build());

      IterableDatasetFinder<CopyableDatasetBase> iterableDatasetFinder = datasetFinder instanceof IterableDatasetFinder
          ? (IterableDatasetFinder<CopyableDatasetBase>) datasetFinder : new IterableDatasetFinderImpl<>(datasetFinder);

      Iterator<CopyableDatasetBase> copyableDatasets =
          new InterruptibleIterator<>(iterableDatasetFinder.getDatasetsIterator(), new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
              return shouldStopGeneratingWorkUnits(workUnitList);
            }
          });

      Iterator<Callable<Void>> callableIterator =
          Iterators.transform(copyableDatasets, new Function<CopyableDatasetBase, Callable<Void>>() {
            @Nullable
            @Override
            public Callable<Void> apply(@Nullable CopyableDatasetBase copyableDataset) {

              IterableCopyableDataset iterableCopyableDataset;
              if (copyableDataset instanceof IterableCopyableDataset) {
                iterableCopyableDataset = (IterableCopyableDataset) copyableDataset;
              } else if (copyableDataset instanceof CopyableDataset) {
                iterableCopyableDataset = new IterableCopyableDatasetImpl((CopyableDataset) copyableDataset);
              } else {
                throw new RuntimeException(String.format("Cannot process %s, can only copy %s or %s.",
                    copyableDataset == null ? null : copyableDataset.getClass().getName(),
                    CopyableDataset.class.getName(), IterableCopyableDataset.class.getName()));
              }

              return new DatasetWorkUnitGenerator(iterableCopyableDataset, sourceFs, targetFs, state, workUnitList,
                  copyConfiguration, minWorkUnitWeight);
            }
          });

      try {
        List<Future<Void>> futures = new IteratorExecutor<>(callableIterator,
            state.getPropAsInt(MAX_CONCURRENT_LISTING_SERVICES, DEFAULT_MAX_CONCURRENT_LISTING_SERVICES),
            ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("Copy-file-listing-pool-%d"))).execute();

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

      log.info(String.format("Created %s workunits ", workUnitList.getWorkUnits().size()));

      copyConfiguration.getCopyContext().logCacheStatistics();

      if (state.contains(SIMULATE) && state.getPropAsBoolean(SIMULATE)) {
        Map<FileSet<CopyEntity>, List<WorkUnit>> copyEntitiesMap = workUnitList.getRawWorkUnitMap();
        log.info("Simulate mode enabled. Will not execute the copy.");
        for (Map.Entry<FileSet<CopyEntity>, List<WorkUnit>> entry : copyEntitiesMap.entrySet()) {
          log.info(String.format("Actions for dataset %s file set %s.", entry.getKey().getDataset().datasetURN(),
              entry.getKey().getName()));
          for (WorkUnit workUnit : entry.getValue()) {
            CopyEntity copyEntity = deserializeCopyEntity(workUnit);
            log.info(copyEntity.explain());
          }
        }
        return Lists.newArrayList();
      }

      List<WorkUnit> flatWorkUnits = workUnitList.getWorkUnits();
      List<? extends WorkUnit> workUnits =
          new WorstFitDecreasingBinPacking(maxSizePerBin).pack(flatWorkUnits, this.weighter);
      log.info(String.format(
          "Bin packed work units. Initial work units: %d, packed work units: %d, max weight per bin: %d, "
              + "max work units per bin: %d.",
          flatWorkUnits.size(), workUnits.size(), maxSizePerBin, maxWorkUnitsPerMultiWorkUnit));
      return Lists.newArrayList(workUnits);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * {@link Runnable} to generate copy listing for one {@link CopyableDataset}.
   */
  @AllArgsConstructor
  private class DatasetWorkUnitGenerator implements Callable<Void> {

    private final IterableCopyableDataset copyableDataset;
    private final FileSystem originFs;
    private final FileSystem targetFs;
    private final State state;
    private final ConcurrentBoundedWorkUnitList workUnitList;
    private final CopyConfiguration copyConfiguration;
    private final long minWorkUnitWeight;

    @Override
    public Void call() {

      if (shouldStopGeneratingWorkUnits(this.workUnitList)) {
        return null;
      }

      try {

        Iterator<FileSet<CopyEntity>> fileSets =
            this.copyableDataset.getFileSetIterator(this.targetFs, this.copyConfiguration);

        while (fileSets.hasNext() && !shouldStopGeneratingWorkUnits(this.workUnitList)) {
          FileSet<CopyEntity> fileSet = fileSets.next();
          String extractId = fileSet.getName().replace(':', '_');
          Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, CopyConfiguration.COPY_PREFIX, extractId);
          List<WorkUnit> workUnitsForPartition = Lists.newArrayList();
          for (CopyEntity copyEntity : fileSet.getFiles()) {

            CopyableDatasetMetadata metadata = new CopyableDatasetMetadata(this.copyableDataset);
            CopyEntity.DatasetAndPartition datasetAndPartition = copyEntity.getDatasetAndPartition(metadata);

            WorkUnit workUnit = new WorkUnit(extract);
            workUnit.addAll(this.state);
            serializeCopyEntity(workUnit, copyEntity);
            serializeCopyableDataset(workUnit, metadata);
            GobblinMetrics.addCustomTagToState(workUnit,
                new Tag<>(CopyEventSubmitterHelper.DATASET_ROOT_METADATA_NAME, this.copyableDataset.datasetURN()));
            workUnit.setProp(ConfigurationKeys.DATASET_URN_KEY, datasetAndPartition.toString());
            workUnit.setProp(SlaEventKeys.DATASET_URN_KEY, this.copyableDataset.datasetURN());
            workUnit.setProp(SlaEventKeys.PARTITION_KEY, copyEntity.getFileSet());
            setWorkUnitWeight(workUnit, copyEntity, minWorkUnitWeight);
            computeAndSetWorkUnitGuid(workUnit);
            workUnitsForPartition.add(workUnit);
          }
          this.workUnitList.addFileSet(fileSet, workUnitsForPartition);
        }
      } catch (IOException ioe) {
        throw new RuntimeException("Failed to generate work units for dataset " + this.copyableDataset.datasetURN(),
            ioe);
      }
      return null;
    }
  }

  private boolean shouldStopGeneratingWorkUnits(ConcurrentBoundedWorkUnitList workUnitList) {
    // Stop generating work units the first time the work unit container rejects a file set due to capacity issues.
    // TODO: more sophisticated stop algorithm.
    return workUnitList.isFull() || workUnitList.hasRejectedFileSet();
  }

  /**
   * @param state a {@link gobblin.configuration.WorkUnitState} carrying properties needed by the returned
   *          {@link Extractor}
   * @return a {@link FileAwareInputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {

    Class<?> copyEntityClass = getCopyEntityClass(state);

    if (CopyableFile.class.isAssignableFrom(copyEntityClass)) {
      CopyableFile copyEntity = (CopyableFile) deserializeCopyEntity(state);
      return extractorForCopyableFile(getSourceFileSystem(state), copyEntity);
    }
    return new EmptyExtractor<>("empty");
  }

  protected Extractor<String, FileAwareInputStream> extractorForCopyableFile(FileSystem fs, CopyableFile cf)
      throws IOException {
    return new FileAwareInputStreamExtractor(fs, cf);
  }

  @Override
  public void shutdown(SourceState state) {}

  protected FileSystem getSourceFileSystem(State state) throws IOException {

    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  private static FileSystem getTargetFileSystem(State state) throws IOException {
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

  private static void computeAndSetWorkUnitGuid(WorkUnit workUnit) throws IOException {
    Guid guid = Guid.fromStrings(workUnit.contains(ConfigurationKeys.CONVERTER_CLASSES_KEY)
        ? workUnit.getProp(ConfigurationKeys.CONVERTER_CLASSES_KEY) : "");
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
  public static Optional<Guid> getWorkUnitGuid(State state) throws IOException {
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

  public static Class<?> getCopyEntityClass(State state) throws IOException {
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
