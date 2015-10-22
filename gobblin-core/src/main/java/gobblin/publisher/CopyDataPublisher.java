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

package gobblin.publisher;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.util.ParallelRunner;
import gobblin.writer.DataWriter;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;


/**
 * An extension to {@link BaseDataPublisher} that publishes data from copy jobs.
 * <p>
 * Lists out all the files/directories under the {@link DataWriter} output directory and moves them to
 * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}
 * </p>
 */
@Slf4j
public class CopyDataPublisher extends BaseDataPublisher {

  public CopyDataPublisher(State state) throws IOException {
    super(state);
  }

  /**
   * See javadoc on the class for functionality.
   * {@inheritDoc}
   * @see gobblin.publisher.BaseDataPublisher#rename(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, int)
   */
  @Override
  protected void rename(Path writerOutputDir, Path publisherOutputDir, int branchId) throws IOException {

    FileSystem fs = this.fileSystemByBranches.get(branchId);
    for (FileStatus fileStatus : fs.listStatus(writerOutputDir)) {
      log.info(String.format("Publishing %s to %s", fileStatus.getPath(), new Path(publisherOutputDir,
          getNewFileName(fileStatus))));
      fs.rename(fileStatus.getPath(), new Path(publisherOutputDir, getNewFileName(fileStatus)));
    }
  }

  protected String getNewFileName(FileStatus fileStatus) {
    return fileStatus.getPath().getName();
  }

  @Override
  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner) throws IOException {
    this.rename(writerOutputDir, publisherOutputDir, branchId);
  }

  @Override
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    Preconditions.checkArgument(workUnitState.contains(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
    return new Path(workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR));
  }
}
