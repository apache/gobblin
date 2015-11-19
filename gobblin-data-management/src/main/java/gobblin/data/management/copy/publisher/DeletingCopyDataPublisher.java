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
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.publisher.CopyDataPublisher;
import gobblin.util.HadoopUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


/**
 * A {@link CopyDataPublisher} that deletes files on the source fileSystem for all the {@link WorkUnitState}s that are
 * successfully committed/published
 */
@Slf4j
public class DeletingCopyDataPublisher extends CopyDataPublisher {

  private final FileSystem sourceFs;

  public DeletingCopyDataPublisher(State state) throws IOException {
    super(state);
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    this.sourceFs = FileSystem.get(URI.create(uri), conf);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    super.publishData(states);
    for (WorkUnitState state : states) {
      if (state.getWorkingState() == WorkingState.COMMITTED) {
        try {
          deleteFilesOnSource(state);
        } catch (Throwable t) {
          log.warn(
              String.format("Failed to delete one or more files on source in %s",
                  state.getProp(CopySource.SERIALIZED_COPYABLE_FILES)), t);
        }
      } else {
        log.info(String.format("Not deleting files %s on source fileSystem as the workunit state is %s.",
            state.getProp(CopySource.SERIALIZED_COPYABLE_FILES), state.getWorkingState()));
      }
    }
  }

  private void deleteFilesOnSource(WorkUnitState state) throws IOException {
    List<CopyableFile> copyableFiles = CopySource.deserializeCopyableFiles(state);
    for (CopyableFile copyableFile : copyableFiles) {
      if (this.sourceFs.exists(copyableFile.getOrigin().getPath())) {
        log.info(String.format("Deleting %s on source fileSystem.", copyableFile.getOrigin().getPath()));
        if (!this.sourceFs.delete(copyableFile.getOrigin().getPath(), true)) {
          throw new IOException("Delete failed for " + copyableFile.getOrigin().getPath());
        }
      }
    }
  }
}
