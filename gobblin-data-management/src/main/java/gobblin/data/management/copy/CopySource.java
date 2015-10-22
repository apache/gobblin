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
import gobblin.data.management.dataset.Dataset;
import gobblin.data.management.partition.Partition;
import gobblin.data.management.retention.dataset.finder.DatasetFinder;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.extract.AbstractSource;
import gobblin.source.extractor.hadoop.InputStreamExtractor;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;


/**
 * {@link gobblin.source.Source} that generates work units from {@link gobblin.data.management.copy.CopyableDataset}s.
 *
 */
public class CopySource extends AbstractSource<String, FileAwareInputStream> {

  /**
   * Parses a {@link gobblin.data.management.copy.CopyableFile} from a work unit containing a serialized
   * {@link gobblin.data.management.copy.CopyableFile}.
   * @param state work unit state.
   * @return Deserialized {@link gobblin.data.management.copy.CopyableFile}.
   * @throws IOException
   */
  public static CopyableFile getCopyableFile(WorkUnitState state) throws IOException {
    return CopyableFile.deserializeCopyableFile(state.getProperties());
  }

  /**
   * Does the following:
   *  1. Instantiate a {@link DatasetFinder}.
   *  2. Find all {@link Dataset} using {@link DatasetFinder}.
   *  3. For each {@link CopyableDataset} get all {@link CopyableFile}s.
   *  4. Create a {@link WorkUnit} per {@link CopyableFile}.
   * @param state see {@link gobblin.configuration.SourceState}
   * @return Work units for copying files.
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {

    List<WorkUnit> workUnits = Lists.newArrayList();

    try {
      DatasetFinder<CopyableDataset> datasetFinder =
          new CopyableDatasetFinder(getSourceFileSystem(state), state.getProperties());
      List<CopyableDataset> copyableDatasets = datasetFinder.findDatasets();

      for (CopyableDataset copyableDataset : copyableDatasets) {

        Collection<Partition<CopyableFile>> partitions =
            copyableDataset.partitionFiles(copyableDataset.getCopyableFiles(getTargetFileSystem(state)));

        for (Partition<CopyableFile> partition : partitions) {
          Extract extract = new Extract(Extract.TableType.SNAPSHOT_ONLY, "gobblin.copy", partition.getName());
          for (CopyableFile copyableFile : partition.getFiles()) {
            WorkUnit workUnit = new WorkUnit(extract);
            workUnit.addAll(state);

            workUnit.setProp(
                ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, 0, 0),
                copyableFile.getDestination().toString());
            state.setProp(CopyableFile.SERIALIZED_COPYABLE_FILE, CopyableFile.serializeCopyableFile(copyableFile));
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
   * @param state a {@link gobblin.configuration.WorkUnitState} carrying properties needed by the returned {@link Extractor}
   * @return a {@link InputStreamExtractor}.
   * @throws IOException
   */
  @Override
  public Extractor<String, FileAwareInputStream> getExtractor(WorkUnitState state) throws IOException {
    CopyableFile copyableFile = CopyableFile.deserializeCopyableFile(state.getProperties());
    return new InputStreamExtractor(getSourceFileSystem(state), Iterators.singletonIterator(copyableFile));
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
}
