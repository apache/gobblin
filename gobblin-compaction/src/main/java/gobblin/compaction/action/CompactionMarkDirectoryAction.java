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

package gobblin.compaction.action;

import gobblin.compaction.event.CompactionSlaEventHelper;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.FileSystemDataset;
import gobblin.metrics.event.EventSubmitter;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;


@Slf4j
@AllArgsConstructor
public class CompactionMarkDirectoryAction implements CompactionCompleteAction<FileSystemDataset> {
  protected State state;
  private CompactionAvroJobConfigurator configurator;
  private FileSystem fs;
  private EventSubmitter eventSubmitter;
  public CompactionMarkDirectoryAction(State state, CompactionAvroJobConfigurator configurator) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = state;
    this.configurator = configurator;
    this.fs = configurator.getFs();
  }

  public void onCompactionJobComplete (FileSystemDataset dataset) throws IOException {
    boolean renamingRequired = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
            MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

    if (renamingRequired) {
      Collection<Path> paths = configurator.getMapReduceInputPaths();
      for (Path path: paths) {
        Path newPath = new Path (path.getParent(), path.getName() + MRCompactor.COMPACTION_RENAME_SOURCE_DIR_SUFFIX);
        log.info("[{}] Renaming {} to {}", dataset.datasetURN(), path, newPath);
        fs.rename(path, newPath);
      }

      // submit events if directory is renamed
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = ImmutableMap.of(CompactionSlaEventHelper.DATASET_URN, dataset.datasetURN(),
            CompactionSlaEventHelper.RENAME_DIR_PATHS, Joiner.on(',').join(paths));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_MARK_DIR_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}
