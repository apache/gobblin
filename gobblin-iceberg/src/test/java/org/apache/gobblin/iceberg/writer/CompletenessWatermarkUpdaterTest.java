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

package org.apache.gobblin.iceberg.writer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.gobblin.completeness.verifier.KafkaAuditCountVerifier;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.time.TimeIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.iceberg.writer.IcebergMetadataWriterConfigKeys.*;
import static org.mockito.Mockito.*;


public class CompletenessWatermarkUpdaterTest {
  static final String TOPIC = "testTopic";
  static final String TABLE_NAME = "testTopic_tableName";
  static final String TIME_ZONE = "America/Los_Angeles";
  static final String AUDIT_CHECK_GRANULARITY = "HOUR";

  static final ZonedDateTime NOW = ZonedDateTime.now(ZoneId.of(TIME_ZONE)).truncatedTo(ChronoUnit.HOURS);
  static final ZonedDateTime ONE_HOUR_AGO = TimeIterator.dec(NOW, TimeIterator.Granularity.valueOf(AUDIT_CHECK_GRANULARITY), 1);
  static final ZonedDateTime TWO_HOUR_AGO = TimeIterator.dec(NOW, TimeIterator.Granularity.valueOf(AUDIT_CHECK_GRANULARITY), 2);
  static final ZonedDateTime THREE_HOUR_AGO = TimeIterator.dec(NOW, TimeIterator.Granularity.valueOf(AUDIT_CHECK_GRANULARITY), 3);

  @Test
  public void testClassicWatermarkOnly() throws IOException {
    TestParams params = createTestParams();

    // Round 1: the expected completion watermark bootstraps to ONE_HOUR_AGO
    //          total completion watermark is not enabled
    KafkaAuditCountVerifier verifier = mockKafkaAuditCountVerifier(ImmutableList.of(
        new AuditCountVerificationResult(TWO_HOUR_AGO, ONE_HOUR_AGO, true /* isCompleteClassic */, false /* isCompleteTotalCount */),
        new AuditCountVerificationResult(THREE_HOUR_AGO, TWO_HOUR_AGO, true, true)));
    CompletenessWatermarkUpdater updater =
        new CompletenessWatermarkUpdater("testTopic", AUDIT_CHECK_GRANULARITY, TIME_ZONE, params.tableMetadata, params.props, params.state, verifier);
    SortedSet<ZonedDateTime> timestamps = new TreeSet<>(Collections.reverseOrder());
    timestamps.add(ONE_HOUR_AGO);
    timestamps.add(TWO_HOUR_AGO);
    boolean includeTotalCountCompletionWatermark = false;
    updater.run(timestamps, includeTotalCountCompletionWatermark);

    validateCompletionWaterMark(ONE_HOUR_AGO, params);
    validateEmptyTotalCompletionWatermark(params);

    // Round 2: the expected completion watermark moves from ONE_HOUR_AGO to NOW
    //          total completion watermark is not enabled
    verifier = mockKafkaAuditCountVerifier(ImmutableList.of(
        new AuditCountVerificationResult(ONE_HOUR_AGO, NOW, true /* isCompleteClassic */, false /* isCompleteTotalCount */),
        new AuditCountVerificationResult(TWO_HOUR_AGO, ONE_HOUR_AGO, true, true),
        new AuditCountVerificationResult(THREE_HOUR_AGO, TWO_HOUR_AGO, true, true)));
    updater.setAuditCountVerifier(verifier);
    timestamps.add(NOW);
    updater.run(timestamps, includeTotalCountCompletionWatermark);

    validateCompletionWaterMark(NOW, params);
    validateEmptyTotalCompletionWatermark(params);
  }

  @Test
  public void testClassicAndTotalCountWatermark() throws IOException {
    TestParams params = createTestParams();

    // Round 1: the expected completion watermark bootstraps to ONE_HOUR_AGO
    //          the expected total completion watermark bootstraps to TOW_HOUR_AGO
    KafkaAuditCountVerifier verifier = mockKafkaAuditCountVerifier(ImmutableList.of(
        new AuditCountVerificationResult(TWO_HOUR_AGO, ONE_HOUR_AGO, true /* isCompleteClassic */, false /* isCompleteTotalCount */),
        new AuditCountVerificationResult(THREE_HOUR_AGO, TWO_HOUR_AGO, true, true)));
    CompletenessWatermarkUpdater updater =
        new CompletenessWatermarkUpdater("testTopic", AUDIT_CHECK_GRANULARITY, TIME_ZONE, params.tableMetadata, params.props, params.state, verifier);
    SortedSet<ZonedDateTime> timestamps = new TreeSet<>(Collections.reverseOrder());
    timestamps.add(ONE_HOUR_AGO);
    timestamps.add(TWO_HOUR_AGO);
    boolean includeTotalCountCompletionWatermark = true;
    updater.run(timestamps, includeTotalCountCompletionWatermark);

    validateCompletionWaterMark(ONE_HOUR_AGO, params);
    validateTotalCompletionWatermark(TWO_HOUR_AGO, params);

    // Round 2: the expected completion watermark moves from ONE_HOUR_AGO to NOW
    //          the expected total completion watermark moves from TOW_HOUR_AGO to ONE_HOUR_AGO
    verifier = mockKafkaAuditCountVerifier(ImmutableList.of(
        new AuditCountVerificationResult(ONE_HOUR_AGO, NOW, true /* isCompleteClassic */, false /* isCompleteTotalCount */),
        new AuditCountVerificationResult(TWO_HOUR_AGO, ONE_HOUR_AGO, true, true),
        new AuditCountVerificationResult(THREE_HOUR_AGO, TWO_HOUR_AGO, true, true)));
    updater.setAuditCountVerifier(verifier);
    timestamps.add(NOW);
    updater.run(timestamps, includeTotalCountCompletionWatermark);

    validateCompletionWaterMark(NOW, params);
    validateTotalCompletionWatermark(ONE_HOUR_AGO, params);
}

  static void validateCompletionWaterMark(ZonedDateTime expectedDT, TestParams params) {
    long expected = expectedDT.toInstant().toEpochMilli();

    // 1. assert updated tableMetadata.completionWatermark
    Assert.assertEquals(params.tableMetadata.completionWatermark, expected);
    // 2. assert updated property
    Assert.assertEquals(params.props.get(COMPLETION_WATERMARK_KEY), String.valueOf(expected));
    Assert.assertEquals(params.props.get(COMPLETION_WATERMARK_TIMEZONE_KEY), TIME_ZONE);
    // 3. assert updated state
    String watermarkKey = String.format(STATE_COMPLETION_WATERMARK_KEY_OF_TABLE,
        params.tableMetadata.table.get().name().toLowerCase(Locale.ROOT));
    Assert.assertEquals(params.state.getProp(watermarkKey), String.valueOf(expected));
  }

  static void validateTotalCompletionWatermark(ZonedDateTime expectedDT, TestParams params) {
    long expected = expectedDT.toInstant().toEpochMilli();

    // 1. expect updated tableMetadata.totalCountCompletionWatermark
    Assert.assertEquals(params.tableMetadata.totalCountCompletionWatermark, expected);
    // 2. expect updated property
    Assert.assertEquals(params.props.get(TOTAL_COUNT_COMPLETION_WATERMARK_KEY), String.valueOf(expected));
    // 3. expect updated state
    String totalCountWatermarkKey = String.format(STATE_TOTAL_COUNT_COMPLETION_WATERMARK_KEY_OF_TABLE,
        params.tableMetadata.table.get().name().toLowerCase(Locale.ROOT));
    Assert.assertEquals(params.state.getProp(totalCountWatermarkKey), String.valueOf(expected));
  }

  static void validateEmptyTotalCompletionWatermark(TestParams params) {
    Assert.assertEquals(params.tableMetadata.totalCountCompletionWatermark, DEFAULT_COMPLETION_WATERMARK);
    Assert.assertNull(params.props.get(TOTAL_COUNT_COMPLETION_WATERMARK_KEY));
    String totalCountWatermarkKey = String.format(STATE_TOTAL_COUNT_COMPLETION_WATERMARK_KEY_OF_TABLE,
        params.tableMetadata.table.get().name().toLowerCase(Locale.ROOT));
    Assert.assertNull(params.state.getProp(totalCountWatermarkKey));
  }

 static class TestParams {
    IcebergMetadataWriter.TableMetadata tableMetadata;
    Map<String, String> props;
    State state;
 }

 static TestParams createTestParams() throws IOException {
   TestParams params = new TestParams();
   params.tableMetadata = new IcebergMetadataWriter.TableMetadata(new Configuration());

   Table table = mock(Table.class);
   when(table.name()).thenReturn(TABLE_NAME);
   params.tableMetadata.table = Optional.of(table);

   params.props = new HashMap<>();
   params.state = new State();

   return params;
 }

 static class AuditCountVerificationResult {
   AuditCountVerificationResult(ZonedDateTime start, ZonedDateTime end, boolean isCompleteClassic, boolean isCompleteTotalCount) {
     this.start = start;
     this.end = end;
     this.isCompleteClassic = isCompleteClassic;
     this.isCompleteTotalCount = isCompleteTotalCount;
   }
   ZonedDateTime start;
   ZonedDateTime end;
   boolean isCompleteClassic;
   boolean isCompleteTotalCount;
 }

 static KafkaAuditCountVerifier mockKafkaAuditCountVerifier(List<AuditCountVerificationResult> resultsToMock)
     throws IOException {
   KafkaAuditCountVerifier verifier = mock(IcebergMetadataWriterTest.TestAuditCountVerifier.class);
   for (AuditCountVerificationResult result : resultsToMock) {
     Mockito.when(verifier.calculateCompleteness(TOPIC, result.start.toInstant().toEpochMilli(), result.end.toInstant().toEpochMilli()))
         .thenReturn(ImmutableMap.of(
             KafkaAuditCountVerifier.CompletenessType.ClassicCompleteness, result.isCompleteClassic,
             KafkaAuditCountVerifier.CompletenessType.TotalCountCompleteness, result.isCompleteTotalCount));
   }
   return verifier;
 }
}
