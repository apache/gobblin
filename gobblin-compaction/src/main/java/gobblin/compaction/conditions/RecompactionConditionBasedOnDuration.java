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

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;


/**
 * An implementation {@link RecompactionCondition} which checks the earliest file modification timestamp from
 * the late output directory. If the earliest file has passed a specified duration and was never cleaned up, a
 * recmpaction will be triggered.
 */
public class RecompactionConditionBasedOnDuration implements RecompactionCondition {

  private final Period duration;
  private static final Logger logger = LoggerFactory.getLogger(RecompactionConditionBasedOnDuration.class);

  public RecompactionConditionBasedOnDuration(Period duration) {
    this.duration = duration;
  }

  public boolean isRecompactionNeeded (DatasetHelper datasetHelper) {
    Optional<DateTime> earliestFileModificationTime = datasetHelper.getEarliestLateFileModificationTime();
    DateTime currentTime = datasetHelper.getCurrentTime();

    if (earliestFileModificationTime.isPresent()) {
      DateTime checkpoint = currentTime.minus(duration);
      logger.info ("Current time is " + currentTime + " Checkpoint is " + checkpoint);
      logger.info ("Ealiest late file has timestamp: " + earliestFileModificationTime.get());
      if (earliestFileModificationTime.get().isBefore(checkpoint)) {
        return true;
      }
    }

    return false;
  }
}
