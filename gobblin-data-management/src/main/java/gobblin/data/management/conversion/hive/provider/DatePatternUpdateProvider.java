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
package gobblin.data.management.conversion.hive.provider;

import java.util.Arrays;

import lombok.ToString;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import gobblin.configuration.ConfigurationKeys;


/**
 * An update provider that parses the date pattern in the {@link Partition} or {@link Table}s physical data location.
 * This parsed date is used as update time.
 */
public class DatePatternUpdateProvider implements HiveUnitUpdateProvider {

  @Override
  public long getUpdateTime(Partition partition) throws UpdateNotFoundExecption {
    return parseDateForLocation(partition.getTPartition().getSd().getLocation());
  }

  @Override
  public long getUpdateTime(Table table) throws UpdateNotFoundExecption {
    return parseDateForLocation(table.getTTable().getSd().getLocation());
  }

  private long parseDateForLocation(String location) throws UpdateNotFoundExecption {
    for (Patterns pattern : Patterns.values()) {
      String dateString = StringUtils.substringAfterLast(location, pattern.prefix);
      if (StringUtils.isNotBlank(dateString)) {
        try {
          return pattern.dateFormat.parseMillis(dateString);
        } catch (IllegalArgumentException | UnsupportedOperationException e) {
          throw new UpdateNotFoundExecption(String.format("Failed parsing date string %s", dateString));
        }

      }
    }
    throw new UpdateNotFoundExecption(String.format("Path %s does not match any date pattern %s", location,
        Arrays.toString(Patterns.values())));
  }

  @ToString
  private enum Patterns {
    DAILY("/daily/", "yyyy/MM/dd"),
    DAILY_LATE("/daily_late/", "yyyy/MM/dd"),
    HOURLY("/hourly/", "yyyy/MM/dd/hh"),
    HOURLY_LATE("/hourly_late/", "yyyy/MM/dd/hh"),
    HOURLY_DEDUPED("/hourly_deduped/", "yyyy/MM/dd/hh");

    private final String prefix;
    private final DateTimeFormatter dateFormat;

    private Patterns(String prefix, String patternString) {
      this.prefix = prefix;
      this.dateFormat =
          DateTimeFormat.forPattern(patternString).withZone(DateTimeZone.forID(ConfigurationKeys.PST_TIMEZONE_NAME));
    }
  }
}
