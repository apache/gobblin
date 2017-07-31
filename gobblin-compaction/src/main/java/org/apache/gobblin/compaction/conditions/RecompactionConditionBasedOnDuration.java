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


import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;



/**
 * An implementation {@link RecompactionCondition} which checks the earliest file modification timestamp from
 * the late output directory. If the earliest file has passed a specified duration and was never cleaned up, a
 * recmpaction will be triggered.
 */
@Alias("RecompactionConditionBasedOnDuration")
public class RecompactionConditionBasedOnDuration implements RecompactionCondition {

  private final Period duration;
  private static final Logger logger = LoggerFactory.getLogger (RecompactionConditionBasedOnDuration.class);

  private RecompactionConditionBasedOnDuration(Dataset dataset) {
    this.duration = getOwnDurationThreshold(dataset);
  }

  @Alias("RecompactBasedOnDuration")
  public static class Factory implements RecompactionConditionFactory {
    @Override public RecompactionCondition createRecompactionCondition (Dataset dataset) {
      return new RecompactionConditionBasedOnDuration (dataset);
    }
  }

  private Period getOwnDurationThreshold (Dataset dataset) {
    String retention = dataset.jobProps().getProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_DURATION,
        MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_DURATION);
    Period period = getPeriodFormatter().parsePeriod(retention);
    return period;
  }

  private static PeriodFormatter getPeriodFormatter() {
    return new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").appendMinutes().appendSuffix("min").toFormatter();
  }

  public boolean isRecompactionNeeded (DatasetHelper datasetHelper) {
    Optional<DateTime> earliestFileModificationTime = datasetHelper.getEarliestLateFileModificationTime();
    DateTime currentTime = datasetHelper.getCurrentTime();

    if (earliestFileModificationTime.isPresent()) {
      DateTime checkpoint = currentTime.minus(duration);

      logger.info ("Current time is " + currentTime + " checkpoint is " + checkpoint);
      logger.info ("Earliest late file has timestamp " + earliestFileModificationTime.get() +
          " inside " + datasetHelper.getDataset().outputLatePath());

      if (earliestFileModificationTime.get().isBefore(checkpoint)) {
        return true;
      }
    }

    return false;
  }
}
