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
package gobblin.data.management.copy.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.ReadyCopyableFileFilter;
import gobblin.util.HadoopUtils;
import gobblin.util.PathUtils;


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
                  state.getProp(CopySource.SERIALIZED_COPYABLE_FILE)), t);
        }
      } else {
        log.info(String.format("Not deleting files %s on source fileSystem as the workunit state is %s.",
            state.getProp(CopySource.SERIALIZED_COPYABLE_FILE), state.getWorkingState()));
      }
    }
  }

  private void deleteFilesOnSource(WorkUnitState state) throws IOException {
    CopyEntity copyEntity = CopySource.deserializeCopyEntity(state);
    if (copyEntity instanceof CopyableFile) {
      HadoopUtils.deletePath(this.sourceFs, ((CopyableFile) copyEntity).getOrigin().getPath(), true);
      HadoopUtils.deletePath(this.sourceFs, PathUtils.addExtension(((CopyableFile) copyEntity).getOrigin().getPath(),
          ReadyCopyableFileFilter.READY_EXTENSION), true);
    }
  }
}
