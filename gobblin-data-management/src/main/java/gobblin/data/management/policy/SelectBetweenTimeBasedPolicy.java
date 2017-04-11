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
package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;

import lombok.ToString;

import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;
import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * Policy used to select versions in a time range. It selects {@link TimestampedDatasetVersion}s that are not older than
 * <code>maxLookBackPeriod</code> and not newer than <code>minLookBackPeriod</code>.
 * <ul>
 * <li> If minLookbackTime is absent, the current time is used as min lookback
 * <li> If maxLookbackTime is absent, an infinite time is used for max lookback
 * </ul>
 *
 */
@Alias("SelectBetweenTimeBasedPolicy")
@ToString
public class SelectBetweenTimeBasedPolicy implements VersionSelectionPolicy<TimestampedDatasetVersion> {

  protected final Optional<Period> minLookBackPeriod;
  protected final Optional<Period> maxLookBackPeriod;

  /**
   * Optional max lookback time. Versions older than this will not be selected
   */
  public static final String TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY = "selection.timeBased.maxLookbackTime";

  /**
   * Optional min lookback time. Versions newer than this will not be selected
   */
  public static final String TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY = "selection.timeBased.minLookbackTime";

  public SelectBetweenTimeBasedPolicy(Config conf) {

    this(conf.hasPath(TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY) ? Optional.of(getLookBackPeriod(conf
        .getString(TIME_BASED_SELECTION_MIN_LOOK_BACK_TIME_KEY))) : Optional.<Period> absent(), conf
        .hasPath(TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY) ? Optional.of(getLookBackPeriod(conf
        .getString(TIME_BASED_SELECTION_MAX_LOOK_BACK_TIME_KEY))) : Optional.<Period> absent());

  }

  public SelectBetweenTimeBasedPolicy(Optional<Period> minLookBackPeriod, Optional<Period> maxLookBackPeriod) {

    this.minLookBackPeriod = minLookBackPeriod;
    this.maxLookBackPeriod = maxLookBackPeriod;
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> listSelectedVersions(List<TimestampedDatasetVersion> allVersions) {
    return Lists.newArrayList(Collections2.filter(allVersions, getSelectionPredicate()));
  }

  private Predicate<TimestampedDatasetVersion> getSelectionPredicate() {
    return new Predicate<TimestampedDatasetVersion>() {
      @Override
      public boolean apply(TimestampedDatasetVersion version) {
        return version.getDateTime()
            .plus(SelectBetweenTimeBasedPolicy.this.maxLookBackPeriod.or(new Period(DateTime.now().getMillis())))
            .isAfterNow()
            && version.getDateTime().plus(SelectBetweenTimeBasedPolicy.this.minLookBackPeriod.or(new Period(0)))
                .isBeforeNow();
      }
    };
  }

  protected static Period getLookBackPeriod(String lookbackTime) {
    PeriodFormatter periodFormatter =
        new PeriodFormatterBuilder().appendYears().appendSuffix("y").appendMonths().appendSuffix("M").appendDays()
            .appendSuffix("d").appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").toFormatter();
    return periodFormatter.parsePeriod(lookbackTime);
  }
}
