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

package gobblin.data.management.policy;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import gobblin.data.management.version.FileSystemDatasetVersion;
import gobblin.data.management.version.TimestampedDatasetVersion;


/**
 * An implementation of {@link VersionSelectionPolicy} which returns versions that are newer than a certain time.
 */
public class TimeBasedSelectionPolicy implements VersionSelectionPolicy<TimestampedDatasetVersion> {

  public static final String TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY = "gobblin.time.based.selection.lookback.time";
  public static final String DEFAULT_LOOK_BACK_TIME = "7d";

  private final Period lookBackPeriod;

  public TimeBasedSelectionPolicy(Properties props) {
    this.lookBackPeriod =
        this.getLookBackPeriod(props.getProperty(TIME_BASED_SELECTION_LOOK_BACK_TIME_KEY, DEFAULT_LOOK_BACK_TIME));
  }

  @Override
  public Class<? extends FileSystemDatasetVersion> versionClass() {
    return TimestampedDatasetVersion.class;
  }

  @Override
  public Collection<TimestampedDatasetVersion> listSelectedVersions(List<TimestampedDatasetVersion> allVersions) {
    return Lists.newArrayList(Collections2.filter(allVersions, new Predicate<FileSystemDatasetVersion>() {
      @Override
      public boolean apply(FileSystemDatasetVersion version) {
        return ((TimestampedDatasetVersion) version).getDateTime().plus(lookBackPeriod).isAfterNow();
      }
    }));
  }

  private Period getLookBackPeriod(String lookbackTime) {
    PeriodFormatter periodFormatter =
        new PeriodFormatterBuilder().appendMonths().appendSuffix("M").appendDays().appendSuffix("d").appendHours()
            .appendSuffix("h").appendMinutes().appendSuffix("m").toFormatter();
    return periodFormatter.parsePeriod(lookbackTime);
  }
}
