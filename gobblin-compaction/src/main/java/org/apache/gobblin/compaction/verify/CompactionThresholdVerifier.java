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

package org.apache.gobblin.compaction.verify;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.gobblin.compaction.action.CompactionGMCEPublishingAction;
import org.apache.gobblin.compaction.conditions.RecompactionConditionBasedOnRatio;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.Path;


/**
 * Compare the source and destination file records' count. Determine if a compaction is needed.
 */
@Slf4j
public class CompactionThresholdVerifier implements CompactionVerifier<FileSystemDataset> {
  private final State state;

  /**
   * Constructor
   */
  public CompactionThresholdVerifier(State state) {
    this.state = state;
  }

  /**
   * There are two record count we are comparing here
   *    1) The new record count in the input folder
   *    2) The record count we compacted previously from last run
   * Calculate two numbers difference and compare with a predefined threshold.
   *
   * (Alternatively we can save the previous record count to a state store. However each input
   * folder is a dataset. We may end up with loading too many redundant job level state for each
   * dataset. To avoid scalability issue, we choose a stateless approach where each dataset tracks
   * record count by themselves and persist it in the file system)
   *
   * @return true if the difference exceeds the threshold or this is the first time compaction or
   * GMCE is enabled but last run there is something wrong when emitting GMCE
   */
  public Result verify(FileSystemDataset dataset) {

    Map<String, Double> thresholdMap = RecompactionConditionBasedOnRatio.
        getDatasetRegexAndRecompactThreshold(
            state.getProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, StringUtils.EMPTY));

    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

    double threshold =
        RecompactionConditionBasedOnRatio.getRatioThresholdByDatasetName(result.getDatasetName(), thresholdMap);
    log.debug("Threshold is {} for dataset {}", threshold, result.getDatasetName());

    InputRecordCountHelper helper = new InputRecordCountHelper(state);
    try {
      double newRecords = 0;
      if (!dataset.isVirtual()) {
        newRecords = helper.calculateRecordCount(Lists.newArrayList(new Path(dataset.datasetURN())));
      }
      double oldRecords = helper.readRecordCount(new Path(result.getDstAbsoluteDir()));
      State datasetState = helper.loadState(new Path(result.getDstAbsoluteDir()));
      if (oldRecords == 0) {
        return new Result(true, "");
      }
      if (state.getPropAsBoolean(ConfigurationKeys.GOBBLIN_METADATA_CHANGE_EVENT_ENABLED, false)
          && !datasetState.getPropAsBoolean(CompactionGMCEPublishingAction.GMCE_EMITTED_KEY, true)) {
        return new Result(true, "GMCE has not sent, need re-compact");
      }
      if (newRecords < oldRecords) {
        return new Result(false, "Illegal state: Current records count should old be smaller.");
      }

      if ((newRecords - oldRecords) / oldRecords > threshold) {
        log.debug("Dataset {} records exceeded the threshold {}", dataset.datasetURN(), threshold);
        return new Result(true, "");
      }

      return new Result(false,
          String.format("%s is failed for dataset %s. Prev=%f, Cur=%f, not reaching to threshold %f", this.getName(),
              result.getDatasetName(), oldRecords, newRecords, threshold));
    } catch (IOException e) {
      return new Result(false, ExceptionUtils.getFullStackTrace(e));
    }
  }

  /**
   * Get compaction threshold verifier name
   */
  public String getName() {
    return this.getClass().getName();
  }

  public boolean isRetriable() {
    return false;
  }
}
