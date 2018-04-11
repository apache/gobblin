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

package org.apache.gobblin.compaction.conditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;

/**
 * An implementation {@link RecompactionCondition} which examines the number of files in the late outputDir
 * If the file count exceeds the file count limit, a recompaction flow is triggered.
 */
@Alias("RecompactionConditionBasedOnFileCount")
public class RecompactionConditionBasedOnFileCount implements RecompactionCondition {

  private final int fileCountLimit;
  private static final Logger logger = LoggerFactory.getLogger (RecompactionConditionBasedOnFileCount.class);

  private RecompactionConditionBasedOnFileCount (Dataset dataset) {
    this.fileCountLimit = getOwnFileCountThreshold (dataset);
  }

  @Alias("RecompactBasedOnFileCount")
  public static class Factory implements RecompactionConditionFactory {
    @Override public RecompactionCondition createRecompactionCondition (Dataset dataset) {
      return new RecompactionConditionBasedOnFileCount (dataset);
    }
  }

  private int getOwnFileCountThreshold (Dataset dataset) {
    int count = dataset.jobProps().getPropAsInt(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FILE_NUM,
        MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_FILE_NUM);
    return count;
  }

  public boolean isRecompactionNeeded (DatasetHelper datasetHelper) {
    long fileNum = datasetHelper.getLateOutputFileCount();
    logger.info ("File count is " + fileNum + " and threshold is " + this.fileCountLimit);
    return (fileNum >= fileCountLimit);
  }
}
