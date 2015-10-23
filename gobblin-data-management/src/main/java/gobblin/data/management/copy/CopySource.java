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
import gobblin.data.management.copy.extractor.InputStreamExtractor;
import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.partition.Partition;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Lists;


/**
 * {@link gobblin.source.Source} that generates work units from {@link gobblin.data.management.copy.CopyableDataset}s.
 *
 */
public class CopySource extends AbstractSource<String, FileAwareInputStream> {

  public static final String DEFAULT_DATASET_PROFILE_CLASS_KEY = CopyableGlobDatasetFinder.class.getCanonicalName();

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
   * In this implementation, one workunit is created for every {@link CopyableFile} found. But the
   * extractor/converters and writers are built to support multiple {@link CopyableFile}s per workunit
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
      for (CopyableDataset copyableDataset : copyableDatasets) {

        Collection<Partition<CopyableFile>> partitions =
            copyableDataset.partitionFiles(copyableDataset.getCopyableFiles(getTargetFileSystem(state)));

        for (Partition<CopyableFile> partition : partitions) {
          Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "gobblin.copy", partition.getName());
          for (CopyableFile copyableFile : partition.getFiles()) {
            WorkUnit workUnit = new WorkUnit(extract);
            workUnit.addAll(state);
            serializeCopyableFile(workUnit, Lists.newArrayList(copyableFile));
            workUnits.add(workUnit);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return workUnits;
  }

  /**
   * @param state a {@link gobblin.configuration.WorkUnitState} carrying properties needed by the returned
   *          {@link Extractor}
   * @return a {@link InputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {

    List<CopyableFile> copyableFiles = deSerializeCopyableFile(state);

    return new InputStreamExtractor(getSourceFileSystem(state), copyableFiles.iterator());
  }

  @Override
  public void shutdown(SourceState state) {
  }

  private FileSystem getSourceFileSystem(State state) throws IOException {

    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return FileSystem.get(URI.create(uri), conf);
  }

  private FileSystem getTargetFileSystem(State state) throws IOException {

    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    return FileSystem.get(URI.create(uri), conf);

  }

  public static void serializeCopyableFile(State state, List<CopyableFile> copyableFiles) throws IOException {
    for (CopyableFile copyableFile : copyableFiles) {
      state.appendToListProp(CopyableFile.SERIALIZED_COPYABLE_FILE, CopyableFile.serializeCopyableFile(copyableFile));
    }
  }

  public static List<CopyableFile> deSerializeCopyableFile(State state) throws IOException {
    List<String> copyableFileStrings = state.getPropAsList(CopyableFile.SERIALIZED_COPYABLE_FILE);
    List<CopyableFile> copyableFiles = Lists.newArrayList();

    for (String fileString : copyableFileStrings) {
      copyableFiles.add(CopyableFile.deserializeCopyableFile(fileString));
    }
    return copyableFiles;
  }
}
