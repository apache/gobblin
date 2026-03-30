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

package org.apache.gobblin.data.management.copy.iceberg;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Utility for building Iceberg partition filter {@link Expression}s over date/datetime ranges.
 *
 * <p>This class is intentionally config-agnostic: callers resolve their own
 * {@link DateTimeFormatter}, start datetime, and lookback count, then delegate to one of
 * the two factory methods:
 * <ul>
 *   <li>{@link #forDays}  — one partition value per calendar day (daily granularity)</li>
 *   <li>{@link #forHours} — one partition value per clock hour (hourly granularity, naturally
 *       crosses midnight)</li>
 * </ul>
 *
 * <p>Both methods return a {@link FilterResult} containing the ordered list of partition
 * value strings (most-recent first) and the combined Iceberg OR expression ready to be
 * passed to a TableScan.
 *
 * <p>The utility is format-agnostic: any valid {@link DateTimeFormatter} pattern works,
 * so tables with partition formats such as {@code yyyy-MM-dd-HH}, {@code dd-MM-yyyy-HH},
 * {@code yyyyMMdd}, or {@code yyyy-MM-dd} are all handled by the same code path.
 *
 * <p>Example — daily lookback with hourly partition format:
 * <pre>{@code
 * DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
 * LocalDateTime start   = LocalDateTime.of(2025, 4, 3, 5, 0);   // 2025-04-03 at 05:00
 *
 * FilterResult result = IcebergPartitionFilterGenerator.forDays("datepartition", start, 3, fmt);
 * // partitionValues → ["2025-04-03-05", "2025-04-02-05", "2025-04-01-05"]
 * }</pre>
 *
 * <p>Example — hourly lookback crossing midnight:
 * <pre>{@code
 * DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
 * LocalDateTime start   = LocalDateTime.of(2025, 4, 1, 1, 0);   // 2025-04-01 at 01:00
 *
 * FilterResult result = IcebergPartitionFilterGenerator.forHours("datepartition", start, 3, fmt);
 * // partitionValues → ["2025-04-01-01", "2025-04-01-00", "2025-03-31-23"]
 * }</pre>
 */
@Slf4j
public class IcebergPartitionFilterGenerator {

  /**
   * Immutable result returned by both factory methods.
   */
  @Getter
  @AllArgsConstructor
  public static class FilterResult {
    /** Ordered list of partition value strings, most-recent first. */
    private final List<String> partitionValues;
    /** Iceberg OR expression that matches any of the partition values. */
    private final Expression filterExpression;
  }

  /**
   * Generate a partition filter by stepping back {@code lookbackDays} calendar days from
   * {@code start}, producing one partition value per day.
   *
   * <p>The hour/minute of {@code start} is preserved at each step.  This means a formatter
   * pattern that includes {@code HH} (e.g. {@code yyyy-MM-dd-HH}) will embed the same hour
   * in every generated value, giving results like:
   * {@code ["2025-04-03-05", "2025-04-02-05", "2025-04-01-05"]}.
   *
   * @param partitionColumn  Iceberg partition column name (e.g. {@code datepartition})
   * @param start            reference datetime; the hour component is used when the formatter
   *                         includes an hour field
   * @param lookbackDays     number of daily partition values to produce; must be &ge; 1
   * @param formatter        applied to each stepped datetime to produce the partition value string
   * @return {@link FilterResult} with partition values and the combined OR expression
   * @throws IllegalArgumentException if {@code lookbackDays} &lt; 1
   */
  public static FilterResult forDays(String partitionColumn, LocalDateTime start,
      int lookbackDays, DateTimeFormatter formatter) {
    if (lookbackDays < 1) {
      throw new IllegalArgumentException(
          String.format("lookbackDays must be >= 1, got: %d", lookbackDays));
    }
    List<String> values = new ArrayList<>(lookbackDays);
    for (int i = 0; i < lookbackDays; i++) {
      String val = start.minusDays(i).format(formatter);
      values.add(val);
      log.debug("Including daily partition: {}={}", partitionColumn, val);
    }
    return new FilterResult(Collections.unmodifiableList(values),
        buildOrExpression(partitionColumn, values));
  }

  /**
   * Generate a partition filter by stepping back {@code lookbackHours} clock hours from
   * {@code start}, producing one partition value per hour.
   *
   * <p>Stepping naturally crosses day boundaries: starting at {@code 2025-04-01T01:00}
   * with {@code lookbackHours=3} produces
   * {@code ["2025-04-01-01", "2025-04-01-00", "2025-03-31-23"]}
   * when the formatter pattern is {@code yyyy-MM-dd-HH}.
   *
   * @param partitionColumn  Iceberg partition column name
   * @param start            reference datetime; stepping moves backwards by one hour per step
   * @param lookbackHours    number of hourly partition values to produce; must be &ge; 1
   * @param formatter        applied to each stepped datetime to produce the partition value string
   * @return {@link FilterResult} with partition values and the combined OR expression
   * @throws IllegalArgumentException if {@code lookbackHours} &lt; 1
   */
  public static FilterResult forHours(String partitionColumn, LocalDateTime start,
      int lookbackHours, DateTimeFormatter formatter) {
    if (lookbackHours < 1) {
      throw new IllegalArgumentException(
          String.format("lookbackHours must be >= 1, got: %d", lookbackHours));
    }
    List<String> values = new ArrayList<>(lookbackHours);
    for (int i = 0; i < lookbackHours; i++) {
      String val = start.minusHours(i).format(formatter);
      values.add(val);
      log.debug("Including hourly partition: {}={}", partitionColumn, val);
    }
    return new FilterResult(Collections.unmodifiableList(values),
        buildOrExpression(partitionColumn, values));
  }

  /**
   * Build an Iceberg OR expression matching any of {@code values} for {@code column}.
   *
   * <p>Returns {@link Expressions#alwaysFalse()} when {@code values} is empty.
   *
   * <p>This method is exposed as {@code public static} so callers that already have a
   * pre-computed list of partition values can build an expression without going through
   * {@link #forDays} or {@link #forHours}.
   *
   * @param column  partition column name used in each equality predicate
   * @param values  partition value strings; order does not affect the expression semantics
   * @return combined Iceberg OR expression
   */
  public static Expression buildOrExpression(String column, List<String> values) {
    Expression expr = null;
    for (String val : values) {
      Expression e = Expressions.equal(column, val);
      expr = (expr == null) ? e : Expressions.or(expr, e);
    }
    return (expr != null) ? expr : Expressions.alwaysFalse();
  }

  private IcebergPartitionFilterGenerator() { /* utility class — no instances */ }
}
