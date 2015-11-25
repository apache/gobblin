/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.data.management.copy.extractor.FileAwareInputStreamExtractor;
import gobblin.data.management.copy.publisher.CopyEventSubmitterHelper;
import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.Partition;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.Tag;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.data.management.util.PathUtils;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * {@link gobblin.source.Source} that generates work units from {@link gobblin.data.management.copy.CopyableDataset}s.
 *
 */
@Slf4j
public class CopySource extends AbstractSource<String, FileAwareInputStream> {

  public static final String DEFAULT_DATASET_PROFILE_CLASS_KEY = CopyableGlobDatasetFinder.class.getCanonicalName();
  private static final String COPY_PREFIX = "gobblin.copy";
  public static final String SERIALIZED_COPYABLE_FILES = COPY_PREFIX + ".serialized.copyable.files";
  public static final String SERIALIZED_COPYABLE_DATASET = COPY_PREFIX + ".serialized.copyable.datasets";

  /**
   * <ul>
   * Does the following:
   * <li>Instantiate a {@link DatasetFinder}.
   * <li>Find all {@link Dataset} using {@link DatasetFinder}.
   * <li>For each {@link CopyableDataset} get all {@link CopyableFile}s.
   * <li>Create a {@link WorkUnit} per {@link CopyableFile}.
   * </ul>
   *
   * <p>
   * In this implementation, one workunit is created for every {@link CopyableFile} found. But the extractor/converters
   * and writers are built to support multiple {@link CopyableFile}s per workunit
   * </p>
   *
   * @param state see {@link gobblin.configuration.SourceState}
   * @return Work units for copying files.
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    List<WorkUnit> workUnits = Lists.newArrayList();
    try {

      DatasetFinder<CopyableDataset> datasetFinder =
          DatasetUtils.instantiateDatasetFinder(state.getProperties(), getSourceFileSystem(state),
              DEFAULT_DATASET_PROFILE_CLASS_KEY);
      List<CopyableDataset> copyableDatasets = datasetFinder.findDatasets();
      FileSystem targetFs = getTargetFileSystem(state);

      for (CopyableDataset copyableDataset : copyableDatasets) {

        Path targetRoot = getTargetRoot(state, datasetFinder, copyableDataset);

        Collection<Partition<CopyableFile>> partitions =
            copyableDataset.partitionFiles(copyableDataset.getCopyableFiles(targetFs, targetRoot));

        for (Partition<CopyableFile> partition : partitions) {
          Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, COPY_PREFIX, partition.getName());
          for (CopyableFile copyableFile : partition.getFiles()) {
            WorkUnit workUnit = new WorkUnit(extract);
            workUnit.addAll(state);
            serializeCopyableFiles(workUnit, Lists.newArrayList(copyableFile));
            serializeCopyableDataset(workUnit, new CopyableDatasetMetadata(copyableDataset, targetRoot));
            GobblinMetrics.addCustomTagToState(workUnit, new Tag<String>(
                CopyEventSubmitterHelper.DATASET_ROOT_METADATA_NAME, copyableDataset.datasetRoot().toString()));
            workUnit.setProp(SlaEventKeys.DATASET_URN_KEY, copyableDataset.datasetRoot().toString());
            workUnit.setProp(SlaEventKeys.PARTITION_KEY, partition.getName());
            workUnit.setProp(SlaEventKeys.ORIGIN_TS_IN_MILLI_SECS_KEY, copyableFile.getFileStatus().getModificationTime());
            workUnits.add(workUnit);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    log.info(String.format("Created %s workunits", workUnits.size()));

    return workUnits;
  }

  /**
   * @param state a {@link gobblin.configuration.WorkUnitState} carrying properties needed by the returned
   *          {@link Extractor}
   * @return a {@link FileAwareInputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {

    List<CopyableFile> copyableFiles = deserializeCopyableFiles(state);

    return new FileAwareInputStreamExtractor(getSourceFileSystem(state), copyableFiles.iterator());
  }

  @Override
  public void shutdown(SourceState state) {
  }

  protected FileSystem getSourceFileSystem(State state) throws IOException {

    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return FileSystem.get(URI.create(uri), conf);
  }

  private FileSystem getTargetFileSystem(State state) throws IOException {
    return WriterUtils.getWriterFS(state, 1, 0);
  }

  private Path getTargetRoot(State state, DatasetFinder<?> datasetFinder, CopyableDataset dataset) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR),
        "Missing property " + ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR);
    Path basePath = new Path(state.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
    Path datasetRelativeToCommonRoot = PathUtils.relativizePath(
        PathUtils.getPathWithoutSchemeAndAuthority(dataset.datasetRoot()),
        PathUtils.getPathWithoutSchemeAndAuthority(datasetFinder.commonDatasetRoot()));
    return new Path(basePath, datasetRelativeToCommonRoot);
  }

  /**
   * Serialize a {@link List} of {@link CopyableFile}s into a {@link State} at {@link #SERIALIZED_COPYABLE_FILES}
   */
  public static void serializeCopyableFiles(State state, List<CopyableFile> copyableFiles) throws IOException {
    state.setProp(SERIALIZED_COPYABLE_FILES, CopyableFile.serializeList(copyableFiles));
  }

  /**
   * Deserialize a {@link List} of {@link CopyableFile}s from a {@link State} at {@link #SERIALIZED_COPYABLE_FILES}
   */
  public static List<CopyableFile> deserializeCopyableFiles(State state) throws IOException {
    return CopyableFile.deserializeList(state.getProp(SERIALIZED_COPYABLE_FILES));
  }

  /**
   * Serialize a {@link CopyableDataset} into a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static void serializeCopyableDataset(State state, CopyableDatasetMetadata copyableDataset) throws IOException {
    state.setProp(SERIALIZED_COPYABLE_DATASET, copyableDataset.serialize());
  }

  /**
   * Deserialize a {@link CopyableDataset} from a {@link State} at {@link #SERIALIZED_COPYABLE_DATASET}
   */
  public static CopyableDatasetMetadata deserializeCopyableDataset(State state) throws IOException {
    return CopyableDatasetMetadata.deserialize(state.getProp(SERIALIZED_COPYABLE_DATASET));
  }
}
