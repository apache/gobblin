/*
 * Copyright (C) 2016-2018 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.compaction.conditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.annotation.Alias;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.dataset.Dataset;

/**
 * An implementation {@link RecompactionCondition} which examines the number of files in the late outputDir
 * If the file count exceeds the file count limit, a recompaction flow is triggered.
 */
@Alias("RecompactionConditionBasedOnFileCount")
public class RecompactionConditionBasedOnFileCount implements RecompactionCondition {

  private final int fileCountLimit;
  private static final Logger logger = LoggerFactory.getLogger (RecompactionConditionBasedOnFileCount.class);

  public RecompactionConditionBasedOnFileCount (Dataset dataset) {
    this.fileCountLimit = getOwnFileCountThreshold (dataset);
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
