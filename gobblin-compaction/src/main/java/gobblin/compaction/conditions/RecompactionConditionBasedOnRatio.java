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


import gobblin.compaction.dataset.DatasetHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * An implementation {@link RecompactionCondition} which examines the late record percentage.
 * If the percent exceeds the limit, a recompaction is triggered.
 */
public class RecompactionConditionBasedOnRatio implements RecompactionCondition {

  private static final Logger logger = LoggerFactory.getLogger(RecompactionConditionBasedOnDuration.class);
  private final double ratio;

  public RecompactionConditionBasedOnRatio(double ratio) {
    this.ratio = ratio;
  }

  public boolean isRecompactionNeeded (DatasetHelper metric) {

    long lateDataCount = metric.getLateOutputRecordCount();
    long nonLateDataCount = metric.getOutputRecordCount();
    double lateDataPercent = lateDataCount * 1.0 /  (lateDataCount + nonLateDataCount);
    logger.info ("Late data ratio is " + lateDataPercent + " and threshold is " + this.ratio);
    if (lateDataPercent > ratio) {
      return true;
    }

    return false;
  }
}