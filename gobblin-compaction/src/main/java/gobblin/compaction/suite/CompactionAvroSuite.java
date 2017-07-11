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

package gobblin.compaction.suite;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.compaction.action.CompactionCompleteFileOperationAction;
import gobblin.compaction.action.CompactionMarkDirectoryAction;
import gobblin.compaction.action.CompactionHiveRegistrationAction;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.verify.CompactionAuditCountVerifier;
import gobblin.compaction.verify.CompactionThresholdVerifier;
import gobblin.compaction.verify.CompactionTimeRangeVerifier;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;

/**
 * A type of {@link CompactionSuite} which implements all components needed for avro file compaction.
 */
@Slf4j
public class CompactionAvroSuite implements CompactionSuite<FileSystemDataset> {
  public static final String SERIALIZE_COMPACTION_FILE_PATH_NAME = "compaction-file-path-name";
  private State state;
  private CompactionAvroJobConfigurator configurator = null;

  /**
   * Constructor
   */
  public CompactionAvroSuite (State state) {
    this.state = state;
  }

  /**
   * Implementation of {@link CompactionSuite#getDatasetsFinderVerifiers()}
   * @return A list of {@link CompactionVerifier} instances which will be verified after
   *         {@link FileSystemDataset} is found but before a {@link gobblin.source.workunit.WorkUnit}
   *         is created.
   */
  public List<CompactionVerifier<FileSystemDataset>> getDatasetsFinderVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new LinkedList<>();
    list.add(new CompactionTimeRangeVerifier(state));
    list.add(new CompactionThresholdVerifier(state));
    list.add(new CompactionAuditCountVerifier(state));
    return list;
  }

  /**
   * Implementation of {@link CompactionSuite#getMapReduceVerifiers()}
   * @return A list of {@link CompactionVerifier} instances which will be verified before
   *         {@link gobblin.compaction.mapreduce.MRCompactionTask} starts the map-reduce job
   */
  public List<CompactionVerifier<FileSystemDataset>> getMapReduceVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new ArrayList<>();
    return list;
  }

  /**
   * Serialize a dataset {@link FileSystemDataset} to a {@link State}
   * @param dataset A dataset needs serialization
   * @param state   A state that is used to save {@link gobblin.dataset.Dataset}
   */
  public void save (FileSystemDataset dataset, State state) {
    state.setProp(SERIALIZE_COMPACTION_FILE_PATH_NAME, dataset.datasetURN());
  }

  /**
   * Deserialize a new {@link FileSystemDataset} from a given {@link State}
   *
   * @param state a type of {@link gobblin.runtime.TaskState}
   * @return A new instance of {@link FileSystemDataset}
   */
  public FileSystemDataset load (final State state) {
    return new FileSystemDataset() {
      @Override
      public Path datasetRoot() {
        return new Path(state.getProp(SERIALIZE_COMPACTION_FILE_PATH_NAME));
      }

      @Override
      public String datasetURN() {
        return state.getProp(SERIALIZE_COMPACTION_FILE_PATH_NAME);
      }
    };
  }

  /**
   * Some post actions are required after compaction job (map-reduce) is finished.
   *
   * @return  A list of {@link CompactionCompleteAction}s which needs to be executed after
   *          map-reduce is done.
   */
  public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() {
    ArrayList<CompactionCompleteAction<FileSystemDataset>> array = new ArrayList<>();
    array.add(new CompactionCompleteFileOperationAction(state, configurator));
    array.add(new CompactionHiveRegistrationAction(state));
    array.add(new CompactionMarkDirectoryAction(state, configurator));
    return array;
  }

  /**
   * Constructs a map-reduce job suitable for avro compaction. The detailed configuration
   * work is delegated to {@link CompactionAvroJobConfigurator#createJob(FileSystemDataset)}
   *
   * @param  dataset a top level input path which contains all avro files those need to be compacted
   * @return a map-reduce job which will compact avro files against {@link gobblin.dataset.Dataset}
   */
  public Job createJob (FileSystemDataset dataset) throws IOException {
    configurator = new CompactionAvroJobConfigurator(this.state);
    return configurator.createJob(dataset);
  }
}
