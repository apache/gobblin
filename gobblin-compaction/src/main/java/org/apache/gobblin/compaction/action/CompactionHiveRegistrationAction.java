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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.compaction.verify.InputRecordCountHelper;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;
import org.apache.gobblin.util.PathUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.mapreduce.MRCompactionTask;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.hive.HiveRegister;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicy;
import org.apache.gobblin.hive.policy.HiveRegistrationPolicyBase;
import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metrics.event.CountEventBuilder;
import org.apache.gobblin.metrics.event.EventSubmitter;


/**
 * Class responsible for hive registration after compaction is complete
 */
@Slf4j
public class CompactionHiveRegistrationAction implements CompactionCompleteAction<FileSystemDataset> {

  public static final String NUM_OUTPUT_FILES = "numOutputFiles";
  public static final String RECORD_COUNT = "recordCount";
  public static final String BYTE_COUNT = "byteCount";
  public static final String DATASET_URN = "datasetUrn";

  private final State state;
  private EventSubmitter eventSubmitter;
  private InputRecordCountHelper helper;

  public CompactionHiveRegistrationAction (State state) {
    if (!(state instanceof WorkUnitState)) {
      throw new UnsupportedOperationException(this.getClass().getName() + " only supports workunit state");
    }
    this.state = state;
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) throws IOException {
    if (dataset.isVirtual()) {
      return;
    }

    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

    long numFiles = state.getPropAsLong(MRCompactionTask.FILE_COUNT, -1);
    CountEventBuilder fileCountEvent = new CountEventBuilder(NUM_OUTPUT_FILES, numFiles);
    fileCountEvent.addMetadata(DATASET_URN, result.getDstAbsoluteDir());
    fileCountEvent.addMetadata(RECORD_COUNT, state.getProp(MRCompactionTask.RECORD_COUNT, "-1"));
    fileCountEvent.addMetadata(BYTE_COUNT, state.getProp(MRCompactionTask.BYTE_COUNT, "-1"));
    if (this.eventSubmitter != null) {
      this.eventSubmitter.submit(fileCountEvent);
    } else {
      log.warn("Will not emit events in {} as EventSubmitter is null", getClass().getName());
    }

    if (!state.contains(ConfigurationKeys.HIVE_REGISTRATION_POLICY)) {
      log.info("Will skip hive registration as {} is not configured.", ConfigurationKeys.HIVE_REGISTRATION_POLICY);
      return;
    }

    try (HiveRegister hiveRegister = HiveRegister.get(state)) {
      state.setProp(KafkaSource.TOPIC_NAME, result.getDatasetName());
      HiveRegistrationPolicy hiveRegistrationPolicy = HiveRegistrationPolicyBase.getPolicy(state);

      List<String> paths = new ArrayList<>();
      Path dstPath = new Path(result.getDstAbsoluteDir());
      if (state.getPropAsBoolean(ConfigurationKeys.RECOMPACTION_WRITE_TO_NEW_FOLDER, false)) {
        //Lazily initialize helper
        this.helper = new InputRecordCountHelper(state);
        long executionCount = helper.readExecutionCount(new Path(result.getDstAbsoluteDir()));
        // Use new output path to do registration
        dstPath = PathUtils.mergePaths(dstPath, new Path(String.format(CompactionCompleteFileOperationAction.COMPACTION_DIRECTORY_FORMAT, executionCount)));
      }
      for (HiveSpec spec : hiveRegistrationPolicy.getHiveSpecs(dstPath)) {
        hiveRegister.register(spec);
        paths.add(spec.getPath().toUri().toASCIIString());
        log.info("Hive registration is done for {}", dstPath.toString());
      }

      // submit events for hive registration
      if (eventSubmitter != null) {
        Map<String, String> eventMetadataMap = ImmutableMap
            .of(CompactionSlaEventHelper.DATASET_URN, dataset.datasetURN(), CompactionSlaEventHelper.HIVE_REGISTRATION_PATHS, Joiner.on(',').join(paths));
        this.eventSubmitter.submit(CompactionSlaEventHelper.COMPACTION_HIVE_REGISTRATION_EVENT, eventMetadataMap);
      }
    }
  }

  public void addEventSubmitter(EventSubmitter submitter) {
    this.eventSubmitter = submitter;
  }
}
