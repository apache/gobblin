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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.gobblin.compaction.action.CompactionCompleteAction;
import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Compaction suite with configurable complete actions
 */
public class CompactionSuiteBaseWithConfigurableCompleteAction extends CompactionSuiteBase {

  private final static String COMPACTION_COMPLETE_ACTIONS = "compaction.complete.actions";

  /**
   * Constructor
   */
  public CompactionSuiteBaseWithConfigurableCompleteAction(State state) {
    super(state);
  }

  /**
   * Some post actions are required after compaction job (map-reduce) is finished.
   *
   * @return A list of {@link CompactionCompleteAction}s which needs to be executed after
   *          map-reduce is done.
   */
  @Override
  public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() throws IOException {
    Preconditions.checkArgument(state.contains(COMPACTION_COMPLETE_ACTIONS));
    ArrayList<CompactionCompleteAction<FileSystemDataset>> compactionCompleteActionsList = new ArrayList<>();
    try {
      for (String s : state.getPropAsList(COMPACTION_COMPLETE_ACTIONS)) {
        compactionCompleteActionsList.add((CompactionCompleteAction<FileSystemDataset>) GobblinConstructorUtils.invokeLongestConstructor(
            Class.forName(s), state, getConfigurator(), new InputRecordCountHelper(state)));
      }
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
    return compactionCompleteActionsList;
  }
}
