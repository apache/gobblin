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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.data.management.copy.iceberg.IcebergPartitionFilterGenerator.FilterResult;


/** Unit tests for {@link IcebergPartitionFilterGenerator}. */
public class IcebergPartitionFilterGeneratorTest {

  private static final String PARTITION_COL = "datepartition";

  // ---------------------------------------------------------------------------
  // forDays — daily stepping
  // ---------------------------------------------------------------------------

  @Test
  public void testForDaysDailyFormat() {
    // Format without hour field → plain date values
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDateTime start = LocalDateTime.of(2025, 4, 3, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 3, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("2025-04-03", "2025-04-02", "2025-04-01"),
        "Daily format should produce plain date values, most-recent first");
    Assert.assertNotNull(result.getFilterExpression(), "Filter expression must not be null");
  }

  @Test
  public void testForDaysHourlyFormat() {
    // Format includes HH → hour from start is embedded in every value
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 3, 5, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 3, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("2025-04-03-05", "2025-04-02-05", "2025-04-01-05"),
        "Each day should carry the same hour from start");
  }

  @Test
  public void testForDaysReversedDateFormat() {
    // dd-MM-yyyy-HH — reversed date with hour
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 3, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 2, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("03-04-2025-00", "02-04-2025-00"),
        "Reversed-date format should format each day correctly");
  }

  @Test
  public void testForDaysCompactFormat() {
    // yyyyMMdd — compact without any separator
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 1, fmt);

    Assert.assertEquals(result.getPartitionValues(), Collections.singletonList("20250401"),
        "Compact format should produce a single compact date value");
  }

  @Test
  public void testForDaysSingleDay() {
    // lookbackDays=1 means exactly the start date, nothing more
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 1, fmt);

    Assert.assertEquals(result.getPartitionValues().size(), 1);
    Assert.assertEquals(result.getPartitionValues().get(0), "2025-04-01");
  }

  @Test
  public void testForDaysValuesAreImmutable() {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    FilterResult result = IcebergPartitionFilterGenerator.forDays(
        PARTITION_COL, LocalDateTime.of(2025, 4, 1, 0, 0), 2, fmt);
    try {
      result.getPartitionValues().add("intruder");
      Assert.fail("Partition values list should be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // correct
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*lookbackDays must be >= 1.*")
  public void testForDaysZeroLookbackThrows() {
    IcebergPartitionFilterGenerator.forDays(
        PARTITION_COL, LocalDateTime.now(), 0, DateTimeFormatter.ISO_LOCAL_DATE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testForDaysNegativeLookbackThrows() {
    IcebergPartitionFilterGenerator.forDays(
        PARTITION_COL, LocalDateTime.now(), -3, DateTimeFormatter.ISO_LOCAL_DATE);
  }

  // ---------------------------------------------------------------------------
  // forHours — hourly stepping
  // ---------------------------------------------------------------------------

  @Test
  public void testForHoursBasic() {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 14, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forHours(PARTITION_COL, start, 3, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("2025-04-01-14", "2025-04-01-13", "2025-04-01-12"),
        "Hourly lookback should step back one hour per entry");
    Assert.assertNotNull(result.getFilterExpression(), "Filter expression must not be null");
  }

  @Test
  public void testForHoursSingleHour() {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 5, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forHours(PARTITION_COL, start, 1, fmt);

    Assert.assertEquals(result.getPartitionValues(), Collections.singletonList("2025-04-01-05"));
  }

  @Test
  public void testForHoursCrossesDateBoundary() {
    // Starting at 01:00 and looking back 3 hours crosses into the previous day
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 1, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forHours(PARTITION_COL, start, 3, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("2025-04-01-01", "2025-04-01-00", "2025-03-31-23"),
        "Hourly stepping must cross midnight into the previous day correctly");
  }

  @Test
  public void testForHoursCrossesMonthBoundary() {
    // Starting at 2025-03-01 00:00 and looking back 2 hours crosses into February
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    LocalDateTime start = LocalDateTime.of(2025, 3, 1, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forHours(PARTITION_COL, start, 3, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("2025-03-01-00", "2025-02-28-23", "2025-02-28-22"),
        "Hourly stepping must cross month boundaries correctly");
  }

  @Test
  public void testForHoursReversedDateFormat() {
    // Verify format-agnosticism for hourly path with reversed date
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("dd-MM-yyyy-HH");
    LocalDateTime start = LocalDateTime.of(2025, 4, 1, 14, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forHours(PARTITION_COL, start, 2, fmt);

    Assert.assertEquals(result.getPartitionValues(),
        Arrays.asList("01-04-2025-14", "01-04-2025-13"));
  }

  @Test
  public void testForHoursValuesAreImmutable() {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
    FilterResult result = IcebergPartitionFilterGenerator.forHours(
        PARTITION_COL, LocalDateTime.of(2025, 4, 1, 5, 0), 2, fmt);
    try {
      result.getPartitionValues().add("intruder");
      Assert.fail("Partition values list should be unmodifiable");
    } catch (UnsupportedOperationException expected) {
      // correct
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*lookbackHours must be >= 1.*")
  public void testForHoursZeroLookbackThrows() {
    IcebergPartitionFilterGenerator.forHours(
        PARTITION_COL, LocalDateTime.now(), 0,
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testForHoursNegativeLookbackThrows() {
    IcebergPartitionFilterGenerator.forHours(
        PARTITION_COL, LocalDateTime.now(), -5,
        DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
  }

  // ---------------------------------------------------------------------------
  // buildOrExpression — raw expression builder
  // ---------------------------------------------------------------------------

  @Test
  public void testBuildOrExpressionEmptyListReturnsAlwaysFalse() {
    Expression expr = IcebergPartitionFilterGenerator.buildOrExpression(
        PARTITION_COL, Collections.emptyList());
    // Iceberg's alwaysFalse() is a singleton — compare by identity for robustness
    Assert.assertSame(expr, Expressions.alwaysFalse(),
        "Empty values should produce alwaysFalse() expression");
  }

  @Test
  public void testBuildOrExpressionSingleValue() {
    Expression expr = IcebergPartitionFilterGenerator.buildOrExpression(
        PARTITION_COL, Collections.singletonList("2025-04-01"));
    Assert.assertNotNull(expr);
    // Single value → equal(...) — no OR wrapping; Iceberg op name is upper-case
    Assert.assertEquals(expr.op().toString(), "EQ");
  }

  @Test
  public void testBuildOrExpressionMultipleValues() {
    List<String> values = Arrays.asList("2025-04-03", "2025-04-02", "2025-04-01");
    Expression expr = IcebergPartitionFilterGenerator.buildOrExpression(PARTITION_COL, values);
    Assert.assertNotNull(expr);
    // Three values → or(or(eq,eq),eq) — top-level op is "OR"
    Assert.assertEquals(expr.op().toString(), "OR");
  }

  @Test
  public void testBuildOrExpressionTwoValues() {
    List<String> values = Arrays.asList("2025-04-02", "2025-04-01");
    Expression expr = IcebergPartitionFilterGenerator.buildOrExpression(PARTITION_COL, values);
    Assert.assertEquals(expr.op().toString(), "OR");
  }

  // ---------------------------------------------------------------------------
  // FilterResult value semantics
  // ---------------------------------------------------------------------------

  @Test
  public void testFilterResultHoldsCorrectValues() {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDateTime start = LocalDateTime.of(2025, 4, 2, 0, 0);

    FilterResult result = IcebergPartitionFilterGenerator.forDays(PARTITION_COL, start, 2, fmt);

    Assert.assertEquals(result.getPartitionValues().size(), 2);
    Assert.assertEquals(result.getPartitionValues().get(0), "2025-04-02");
    Assert.assertEquals(result.getPartitionValues().get(1), "2025-04-01");
    Assert.assertNotNull(result.getFilterExpression());
  }
}
