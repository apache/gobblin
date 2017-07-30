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

package gobblin.compaction.verify;


import com.google.common.collect.Lists;
import gobblin.compaction.conditions.RecompactionConditionBasedOnRatio;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

/**
 * Compare the source and destination avro records. Determine if a compaction is needed.
 */
@Slf4j
public class CompactionThresholdVerifier implements CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_THRESHOLD = "compaction-verifier-threshold";
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
   * @return true iff the difference exceeds the threshold or this is the first time compaction
   */
  public boolean verify (FileSystemDataset dataset) {

    Map<String, Double> thresholdMap = RecompactionConditionBasedOnRatio.
            getDatasetRegexAndRecompactThreshold (state.getProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET,
                    StringUtils.EMPTY));

    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);

    double threshold = RecompactionConditionBasedOnRatio.getRatioThresholdByDatasetName (result.getDatasetName(), thresholdMap);
    log.info ("Threshold is {} for dataset {}", threshold, result.getDatasetName());

    InputRecordCountHelper helper = new InputRecordCountHelper(state);
    try {
      double newRecords = helper.calculateRecordCount (Lists.newArrayList(new Path(dataset.datasetURN())));
      double oldRecords = InputRecordCountHelper.readRecordCount (helper.getFs(), new Path(result.getDstAbsoluteDir()));

      log.info ("Dataset {} : previous records {}, current records {}", dataset.datasetURN(), oldRecords, newRecords);
      if (oldRecords == 0) {
        return true;
      }
      if ((newRecords - oldRecords) / oldRecords > threshold) {
        log.info ("Dataset {} records exceeded the threshold {}", dataset.datasetURN(), threshold);
        return true;
      }
    } catch (IOException e) {
      log.error(e.toString());
    }
    return false;
  }

  /**
   * Get compaction threshold verifier name
   */
  public String getName() {
    return COMPACTION_VERIFIER_THRESHOLD;
  }

  public boolean isRetriable () {
    return false;
  }
}
