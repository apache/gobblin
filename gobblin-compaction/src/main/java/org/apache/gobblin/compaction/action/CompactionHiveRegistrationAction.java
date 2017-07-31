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

package org.apache.gobblin.compaction.action;

import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metrics.event.EventSubmitter;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;


/**
 * Class responsible for hive registration after compaction is complete
 */
@Slf4j
public class CompactionHiveRegistrationAction implements CompactionCompleteAction<FileSystemDataset> {
  private final State state;
  private EventSubmitter eventSubmitter;
  public CompactionHiveRegistrationAction (State state) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = state;
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (state.contains(ConfigurationKeys.HIVE_REGISTRATION_POLICY)) {
      HiveRegister hiveRegister = HiveRegister.get(state);
      HiveRegistrationPolicy hiveRegistrationPolicy = HiveRegistrationPolicyBase.getPolicy(state);
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

      List<String> paths = new ArrayList<>();
      for (HiveSpec spec : hiveRegistrationPolicy.getHiveSpecs(new Path(result.getDstAbsoluteDir()))) {
        hiveRegister.register(spec);
        paths.add(spec.getPath().toUri().toASCIIString());
        log.info("Hive registration is done for {}", result.getDstAbsoluteDir());
      }

      // submit events for hive registration
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = ImmutableMap.of(CompactionSlaEventHelper.DATASET_URN, dataset.datasetURN(),
            CompactionSlaEventHelper.HIVE_REGISTRATION_PATHS, Joiner.on(',').join(paths));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_HIVE_REGISTRATION_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}
