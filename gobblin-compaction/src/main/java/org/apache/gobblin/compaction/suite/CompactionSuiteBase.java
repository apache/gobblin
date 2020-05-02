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

package org.apache.gobblin.compaction.suite;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.compaction.action.CompactionCompleteAction;
import org.apache.gobblin.compaction.action.CompactionCompleteFileOperationAction;
import org.apache.gobblin.compaction.action.CompactionHiveRegistrationAction;
import org.apache.gobblin.compaction.action.CompactionMarkDirectoryAction;
import org.apache.gobblin.compaction.mapreduce.CompactionJobConfigurator;
import org.apache.gobblin.compaction.verify.CompactionAuditCountVerifier;
import org.apache.gobblin.compaction.verify.CompactionThresholdVerifier;
import org.apache.gobblin.compaction.verify.CompactionTimeRangeVerifier;
import org.apache.gobblin.compaction.verify.CompactionVerifier;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;
import org.apache.hadoop.mapreduce.Job;


/**
 * A type of {@link CompactionSuite} which implements all components needed for file compaction.
 * The format-specific implementation is contained in the impl. of {@link CompactionJobConfigurator}
 */
@Slf4j
public class CompactionSuiteBase implements CompactionSuite<FileSystemDataset> {

  protected State state;
  /**
   * Require lazy evaluation for now to support feature in
   * {@link org.apache.gobblin.compaction.source.CompactionSource#optionalInit(SourceState)}
   */
  private CompactionJobConfigurator configurator;
  private static final Gson GSON = GsonInterfaceAdapter.getGson(FileSystemDataset.class);
  private static final String SERIALIZED_DATASET = "compaction.serializedDataset";

  /**
   * Constructor
   */
  public CompactionSuiteBase(State state) {
    this.state = state;
  }

  /**
   * Implementation of {@link CompactionSuite#getDatasetsFinderVerifiers()}
   * @return A list of {@link CompactionVerifier} instances which will be verified after
   *         {@link FileSystemDataset} is found but before a {@link org.apache.gobblin.source.workunit.WorkUnit}
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
   *         {@link org.apache.gobblin.compaction.mapreduce.MRCompactionTask} starts the map-reduce job
   */
  public List<CompactionVerifier<FileSystemDataset>> getMapReduceVerifiers() {
    return new ArrayList<>();
  }

  /**
   * Serialize a dataset {@link FileSystemDataset} to a {@link State}
   * @param dataset A dataset needs serialization
   * @param state   A state that is used to save {@link org.apache.gobblin.dataset.Dataset}
   */
  public void save(FileSystemDataset dataset, State state) {
    state.setProp(SERIALIZED_DATASET, GSON.toJson(dataset));
  }

  /**
   * Deserialize a new {@link FileSystemDataset} from a given {@link State}
   *
   * @param state a type of {@link org.apache.gobblin.runtime.TaskState}
   * @return A new instance of {@link FileSystemDataset}
   */
  public FileSystemDataset load(final State state) {
    return GSON.fromJson(state.getProp(SERIALIZED_DATASET), FileSystemDataset.class);
  }

  /**
   * Some post actions are required after compaction job (map-reduce) is finished.
   *
   * @return A list of {@link CompactionCompleteAction}s which needs to be executed after
   *          map-reduce is done.
   */
  public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() throws IOException {
    ArrayList<CompactionCompleteAction<FileSystemDataset>> compactionCompleteActionsList = new ArrayList<>();
    compactionCompleteActionsList.add(new CompactionCompleteFileOperationAction(state, getConfigurator()));
    compactionCompleteActionsList.add(new CompactionHiveRegistrationAction(state));
    compactionCompleteActionsList.add(new CompactionMarkDirectoryAction(state, getConfigurator()));
    return compactionCompleteActionsList;
  }

  /**
   * Constructs a map-reduce job suitable for compaction. The detailed format-specific configuration
   * work is delegated to {@link CompactionJobConfigurator#createJob(FileSystemDataset)}
   *
   * @param  dataset a top level input path which contains all files those need to be compacted
   * @return a map-reduce job which will compact files against {@link org.apache.gobblin.dataset.Dataset}
   */
  public Job createJob(FileSystemDataset dataset) throws IOException {
    return getConfigurator().createJob(dataset);
  }

  protected CompactionJobConfigurator getConfigurator() {
    if (configurator == null) {
      synchronized (this) {
        configurator = CompactionJobConfigurator.instantiateConfigurator(this.state);
      }
    }
    return configurator;
  }
}
