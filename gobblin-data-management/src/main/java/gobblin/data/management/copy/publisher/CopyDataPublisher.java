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

package gobblin.data.management.copy.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.SerializableCopyableDataset;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder;
import gobblin.data.management.util.PathUtils;
import gobblin.publisher.DataPublisher;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


/**
 * A {@link DataPublisher} to {@link CopyableFile}s from task output to final destination.
 */
@Slf4j
public class CopyDataPublisher extends DataPublisher {

  private Path writerOutputDir;
  private FileSystem fs;

  /**
   * Build a new {@link CopyDataPublisher} from {@link State}. The constructor expects the following to be set in the
   * {@link State},
   * <ul>
   * <li>{@link ConfigurationKeys#WRITER_OUTPUT_DIR}
   * <li>{@link ConfigurationKeys#WRITER_FILE_SYSTEM_URI}
   * </ul>
   *
   */
  public CopyDataPublisher(State state) throws IOException {
    super(state);
    Configuration conf = new Configuration();
    String uri = this.state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);

    this.fs = FileSystem.get(URI.create(uri), conf);

    FileAwareInputStreamDataWriterBuilder.setJobSpecificOutputPaths(state);

    this.writerOutputDir = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    /*
     * This mapping is used to set WorkingState of all {@link WorkUnitState}s to {@link
     * WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published
     */
    Multimap<CopyableDataset, WorkUnitState> datasets = getDatasetRoots(states);

    boolean allDatasetsPublished = true;
    for (CopyableDataset copyableDataset : datasets.keySet()) {
      try {
        this.publishDataset(copyableDataset, datasets.get(copyableDataset));
      } catch (Throwable e) {
        // TODO submit failure events here
        log.error("Failed to publish " + copyableDataset.datasetTargetRoot(), e);
        allDatasetsPublished = false;
      }
    }

    fs.delete(writerOutputDir, true);

    if (!allDatasetsPublished) {
      throw new IOException("Not all datasets published successfully");
    }
  }

  /**
   * Create a {@link Multimap} that maps a {@link CopyableDataset} to all {@link WorkUnitState}s that belong to this
   * {@link CopyableDataset}. This mapping is used to set WorkingState of all {@link WorkUnitState}s to
   * {@link WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published.
   */
  private Multimap<CopyableDataset, WorkUnitState> getDatasetRoots(Collection<? extends WorkUnitState> states)
      throws IOException {
    Multimap<CopyableDataset, WorkUnitState> datasetRoots = ArrayListMultimap.create();

    for (WorkUnitState workUnitState : states) {
      CopyableDataset copyableDataset =
          SerializableCopyableDataset.deserialize(workUnitState.getProp(CopySource.SERIALIZED_COPYABLE_DATASET));

      datasetRoots.put(copyableDataset, workUnitState);

    }
    return datasetRoots;
  }

  /**
   * Publish data for a {@link CopyableDataset}.
   *
   */
  private void publishDataset(CopyableDataset copyableDataset, Collection<WorkUnitState> datasetWorkUnitStates)
      throws IOException {

    Path datasetWriterOutputPath = new Path(writerOutputDir, PathUtils.withoutLeadingSeparator(copyableDataset.datasetTargetRoot()));

    log.info(String.format("Publishing dataset from %s to %s", datasetWriterOutputPath, copyableDataset.datasetTargetRoot()));

    HadoopUtils.renameRecursively(fs, datasetWriterOutputPath, copyableDataset.datasetTargetRoot());

    fs.delete(datasetWriterOutputPath, true);

    for (WorkUnitState wus : datasetWorkUnitStates) {
      if (wus.getWorkingState() == WorkingState.SUCCESSFUL) {
        wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void initialize() throws IOException {
  }
}
