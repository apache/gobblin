/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.policies.time;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.qualitychecker.row.RowLevelPolicy;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;


/**
 * An abstract {@link RowLevelPolicy} for checking a record's timestamp against the earliest allowed timestamp.
 * Records whose timestamps are earlier than the earliest allowed timestamp will fail.
 *
 * @author Ziyang Liu
 */
public abstract class RecordTimestampLowerBoundPolicy extends RowLevelPolicy {

  public static final String RECORD_MAX_ALLOWED_TIME_AGO = "record.max.allowed.time.ago";
  public static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder().appendMonths().appendSuffix("m")
      .appendDays().appendSuffix("d").appendHours().appendSuffix("h").toFormatter();

  @SuppressWarnings("rawtypes")
  protected final TimeBasedWriterPartitioner partitioner;
  protected final DateTimeZone timeZone;
  protected final Optional<Long> earliestAllowedTimestamp;

  public RecordTimestampLowerBoundPolicy(State state, Type type) {
    super(state, type);
    this.partitioner = getPartitioner();
    this.timeZone = DateTimeZone.forID(
        state.getProp(ConfigurationKeys.QUALITY_CHECKER_TIMEZONE, ConfigurationKeys.DEFAULT_QUALITY_CHECKER_TIMEZONE));
    this.earliestAllowedTimestamp = getEarliestAllowedTimestamp();
  }

  private Optional<Long> getEarliestAllowedTimestamp() {
    if (!this.state.contains(RECORD_MAX_ALLOWED_TIME_AGO)) {
      return Optional.<Long> absent();
    } else {
      DateTime currentTime = new DateTime(this.timeZone);
      String maxTimeAgoStr = this.state.getProp(RECORD_MAX_ALLOWED_TIME_AGO);
      Period maxTimeAgo = PERIOD_FORMATTER.parsePeriod(maxTimeAgoStr);
      return Optional.of(currentTime.minus(maxTimeAgo).getMillis());
    }
  }

  protected abstract TimeBasedWriterPartitioner<?> getPartitioner();

  @Override
  public Result executePolicy(Object record) {
    @SuppressWarnings("unchecked")
    long recordTimestamp = this.partitioner.getRecordTimestamp(record);
    if (this.earliestAllowedTimestamp.isPresent() && recordTimestamp < this.earliestAllowedTimestamp.get()) {
      return RowLevelPolicy.Result.FAILED;
    } else {
      return RowLevelPolicy.Result.PASSED;
    }
  }

}
