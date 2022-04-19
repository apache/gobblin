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

import com.google.common.base.Splitter;
import java.util.List;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.gobblin.compaction.dataset.TimeBasedSubDirDatasetsFinder;
import org.apache.gobblin.compaction.event.CompactionSlaEventHelper;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;


/**
 * A simple class which verify current dataset belongs to a specific time range. Will skip doing
 * compaction if dataset is not in a correct time range.
 */

@Slf4j
@AllArgsConstructor
public class CompactionTimeRangeVerifier implements CompactionVerifier<FileSystemDataset> {
  public final static String COMPACTION_VERIFIER_TIME_RANGE = COMPACTION_VERIFIER_PREFIX + "time-range";

  protected State state;

  public Result verify(FileSystemDataset dataset) {
    final DateTime earliest;
    final DateTime latest;
    try {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      DateTime folderTime = result.getTime();
      DateTimeZone timeZone = DateTimeZone.forID(
          this.state.getProp(MRCompactor.COMPACTION_TIMEZONE, MRCompactor.DEFAULT_COMPACTION_TIMEZONE));
      DateTime compactionStartTime =
          new DateTime(this.state.getPropAsLong(CompactionSource.COMPACTION_INIT_TIME), timeZone);
      PeriodFormatter formatter = new PeriodFormatterBuilder().appendMonths()
          .appendSuffix("m")
          .appendDays()
          .appendSuffix("d")
          .appendHours()
          .appendSuffix("h")
          .toFormatter();

      // Dataset name is like 'Identity/MemberAccount' or 'PageViewEvent'
      String datasetName = result.getDatasetName();

      // get earliest time
      String maxTimeAgoStrList = this.state.getProp(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MAX_TIME_AGO,
          TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO);
      String maxTimeAgoStr = getMatchedLookbackTime(datasetName, maxTimeAgoStrList,
          TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MAX_TIME_AGO);
      Period maxTimeAgo = formatter.parsePeriod(maxTimeAgoStr);
      earliest = compactionStartTime.minus(maxTimeAgo);

      // get latest time
      String minTimeAgoStrList = this.state.getProp(TimeBasedSubDirDatasetsFinder.COMPACTION_TIMEBASED_MIN_TIME_AGO,
          TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO);
      String minTimeAgoStr = getMatchedLookbackTime(datasetName, minTimeAgoStrList,
          TimeBasedSubDirDatasetsFinder.DEFAULT_COMPACTION_TIMEBASED_MIN_TIME_AGO);
      Period minTimeAgo = formatter.parsePeriod(minTimeAgoStr);
      latest = compactionStartTime.minus(minTimeAgo);

      // get latest last run start time, we want to limit the duration between two compaction for the same dataset
      if (state.contains(TimeBasedSubDirDatasetsFinder.MIN_RECOMPACTION_DURATION)) {
        String minDurationStrList = this.state.getProp(TimeBasedSubDirDatasetsFinder.MIN_RECOMPACTION_DURATION);
        String minDurationStr = getMatchedLookbackTime(datasetName, minDurationStrList,
            TimeBasedSubDirDatasetsFinder.DEFAULT_MIN_RECOMPACTION_DURATION);
        Period minDurationTime = formatter.parsePeriod(minDurationStr);
        DateTime latestEligibleCompactTime = compactionStartTime.minus(minDurationTime);
        InputRecordCountHelper helper = new InputRecordCountHelper(state);
        State compactState = helper.loadState(new Path(result.getDstAbsoluteDir()));
        if (compactState.contains(CompactionSlaEventHelper.LAST_RUN_START_TIME)
            && compactState.getPropAsLong(CompactionSlaEventHelper.LAST_RUN_START_TIME)
            > latestEligibleCompactTime.getMillis()) {
          log.warn("Last compaction for {} is {}, which is not before latestEligibleCompactTime={}",
              dataset.datasetRoot(),
              new DateTime(compactState.getPropAsLong(CompactionSlaEventHelper.LAST_RUN_START_TIME), timeZone),
              latestEligibleCompactTime);
          return new Result(false,
              "Last compaction for " + dataset.datasetRoot() + " is not before" + latestEligibleCompactTime);
        }
      }

      if (earliest.isBefore(folderTime) && latest.isAfter(folderTime)) {
        log.debug("{} falls in the user defined time range", dataset.datasetRoot());
        return new Result(true, "");
      }
    } catch (Exception e) {
      log.error("{} cannot be verified because of {}", dataset.datasetRoot(), ExceptionUtils.getFullStackTrace(e));
      return new Result(false, e.toString());
    }
    return new Result(false, dataset.datasetRoot() + " is not in between " + earliest + " and " + latest);
  }

  public String getName() {
    return COMPACTION_VERIFIER_TIME_RANGE;
  }

  public boolean isRetriable() {
    return false;
  }

  /**
   * Find the correct lookback time for a given dataset.
   *
   * @param datasetsAndLookBacks Lookback string for multiple datasets. Datasets is represented by Regex pattern.
   *                             Multiple 'datasets and lookback' pairs were joined by semi-colon. A default
   *                             lookback time can be given without any Regex prefix. If nothing found, we will use
   *                             {@param sysDefaultLookback}.
   *
   *                             Example Format: [Regex1]:[T1];[Regex2]:[T2];[DEFAULT_T];[Regex3]:[T3]
   *                             Ex. Identity.*:1d2h;22h;BizProfile.BizCompany:3h (22h is default lookback time)
   *
   * @param sysDefaultLookback If user doesn't specify any lookback time for {@param datasetName}, also there is no default
   *                           lookback time inside {@param datasetsAndLookBacks}, this system default lookback time is return.
   *
   * @param datasetName A description of dataset without time partition information. Example 'Identity/MemberAccount' or 'PageViewEvent'
   * @return The lookback time matched with given dataset.
   */
  public static String getMatchedLookbackTime(String datasetName, String datasetsAndLookBacks,
      String sysDefaultLookback) {
    String defaultLookback = sysDefaultLookback;

    for (String entry : Splitter.on(";").trimResults().omitEmptyStrings().splitToList(datasetsAndLookBacks)) {
      List<String> datasetAndLookbackTime = Splitter.on(":").trimResults().omitEmptyStrings().splitToList(entry);
      if (datasetAndLookbackTime.size() == 1) {
        defaultLookback = datasetAndLookbackTime.get(0);
      } else if (datasetAndLookbackTime.size() == 2) {
        String regex = datasetAndLookbackTime.get(0);
        if (Pattern.compile(regex).matcher(datasetName).find()) {
          return datasetAndLookbackTime.get(1);
        }
      } else {
        log.error("Invalid format in {}, {} cannot find its lookback time", datasetsAndLookBacks, datasetName);
      }
    }
    return defaultLookback;
  }
}
