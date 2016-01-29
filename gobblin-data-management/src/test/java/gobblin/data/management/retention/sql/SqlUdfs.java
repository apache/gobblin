/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.retention.sql;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import com.sun.tools.javac.util.StringUtils;


/**
 * Holds User defined functions used by Derby db.
 */
@Slf4j
public final class SqlUdfs {

  /**
   * Returns a value after subtracting a {@link Timestamp} from another. Value is in the {@link TimeUnit} provided in
   * <code>unit</code> string.
   *
   * @param timestamp1 first {@link Timestamp}
   * @param timestamp2 second {@link Timestamp}
   * @param unit for the difference. Any {@link TimeUnit#values()} are supported units.
   * @return
   */
  public static long timestamp_diff(Timestamp timestamp1, Timestamp timestamp2, String unit) {

    return date_diff(timestamp1.getTime(), timestamp2.getTime(), unit);
  }

  private static long date_diff(long timestamp1, long timestamp2, String unitString) {

    try {
      TimeUnit unit = TimeUnit.valueOf(TimeUnit.class, StringUtils.toUpperCase(unitString));
      return unit.convert(timestamp1 - timestamp2, TimeUnit.MILLISECONDS);
    } catch (IllegalArgumentException e) {
      log.error("Valid input for unitString is java.util.concurrent.TimeUnit", e);
    }

    return 0l;
  }

}
