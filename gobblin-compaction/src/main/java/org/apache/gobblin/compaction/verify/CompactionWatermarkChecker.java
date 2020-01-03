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

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.time.TimeIterator;


/**
 * A {@link CompactionAuditCountVerifier} to report compaction watermarks based on verification
 * result
 *
 * <p> A {@code watermarkTime} is the previous time of {@value CompactionSource#COMPACTION_INIT_TIME}. It
 * can be computed in different {@link org.apache.gobblin.time.TimeIterator.Granularity}. For example, if
 * compaction init time is 2019/12/01 18:16:00.000, its compaction watermark in minute granularity is the
 * last millis of previous minute, 2019/12/01 18:15:59.999, and watermark in day granularity is the last
 * millis of previous day, 2019/11/30 23:59:59.999.
 *
 * <p> The checker will report {@code watermarkTime} in epoc millis as {@value COMPACTION_WATERMARK}
 * regardless of audit counts. If audit counts match, it will also report the time in epoc millis
 * as {@value COMPLETION_COMPACTION_WATERMARK}
 */
@Slf4j
public class CompactionWatermarkChecker extends CompactionAuditCountVerifier {

  public static final String TIME_FORMAT = "compactionWatermarkChecker.timeFormat";
  public static final String COMPACTION_WATERMARK = "compactionWatermark";
  public static final String COMPLETION_COMPACTION_WATERMARK = "completionAndCompactionWatermark";

  private final long watermarkTime;
  private final String precedingTimeDatasetPartitionName;

  public CompactionWatermarkChecker(State state) {
    super(state);
    ZonedDateTime compactionTime = ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(state.getPropAsLong(CompactionSource.COMPACTION_INIT_TIME)), zone);
    ZonedDateTime precedingTime = TimeIterator.dec(compactionTime, granularity, 1);
    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(state.getProp(TIME_FORMAT));
    precedingTimeDatasetPartitionName = timeFormatter.format(precedingTime);
    watermarkTime = getWatermarkTimeMillis(compactionTime, granularity);
  }

  @VisibleForTesting
  static long getWatermarkTimeMillis(ZonedDateTime compactionTime, TimeIterator.Granularity granularity) {
    ZonedDateTime startOfMinute = compactionTime.withSecond(0).with(ChronoField.MILLI_OF_SECOND, 0);
    ZonedDateTime startOfTimeGranularity = startOfMinute;
    switch (granularity) {
      case MINUTE:
        break;
      case HOUR:
        startOfTimeGranularity = startOfMinute.withMinute(0);
        break;
      case DAY:
        startOfTimeGranularity = startOfMinute.withHour(0).withMinute(0);
        break;
      case MONTH:
        startOfTimeGranularity = startOfMinute.withDayOfMonth(1).withHour(0).withMinute(0);
        break;
    }
    // The last millis of the start granularity
    return startOfTimeGranularity.minus(1, ChronoUnit.MILLIS).toInstant().toEpochMilli();
  }

  @Override
  public Result verify(FileSystemDataset dataset) {
    Result res = super.verify(dataset);
    if (!dataset.datasetRoot().toString().contains(precedingTimeDatasetPartitionName)) {
      return res;
    }

    // set compaction watermark
    this.state.setProp(COMPACTION_WATERMARK, watermarkTime);
    if (enabled && res.isSuccessful()) {
      log.info("Set dataset {} complete and compaction watermark {}", dataset.datasetRoot(), watermarkTime);
      // If it also passed completeness check
      this.state.setProp(COMPLETION_COMPACTION_WATERMARK, watermarkTime);
    } else {
      log.info("Set dataset {} compaction watermark {}", dataset.datasetRoot(), watermarkTime);
    }
    return res;
  }
}
