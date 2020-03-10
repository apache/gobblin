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

import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.compaction.source.CompactionSource;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.dataset.SimpleFileSystemDataset;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.time.TimeIterator;


public class CompactionWatermarkCheckerTest {

  private final ZoneId zone = ZoneId.of("America/Los_Angeles");

  @Test
  public void testGetWatermark() {
    // 2019/12/01 18:16:00.000
    ZonedDateTime time = ZonedDateTime.of(2019, 12, 1, 18, 16, 0, 0, zone);
    // minute watermark is 2019/12/01 18:15:59.999
    Assert.assertEquals(CompactionWatermarkChecker.getWatermarkTimeMillis(time, TimeIterator.Granularity.MINUTE),
        1575252959999L);
    // hour watermark is 2019/12/01 17:59:59.999
    Assert.assertEquals(CompactionWatermarkChecker.getWatermarkTimeMillis(time, TimeIterator.Granularity.HOUR),
        1575251999999L);
    // day watermark is 2019/11/30 23:59:59.999
    Assert.assertEquals(CompactionWatermarkChecker.getWatermarkTimeMillis(time, TimeIterator.Granularity.DAY),
        1575187199999L);
    // month watermark is 2019/11/30 23:59:59.999
    Assert.assertEquals(CompactionWatermarkChecker.getWatermarkTimeMillis(time, TimeIterator.Granularity.MONTH),
        1575187199999L);
  }

  @Test
  public void testVerify() {
    ZonedDateTime time = ZonedDateTime.of(2019, 12, 1, 18, 16, 0, 0, zone);
    State state = new State();
    state.setProp(CompactionSource.COMPACTION_INIT_TIME, time.toInstant().toEpochMilli());
    state.setProp(CompactionAuditCountVerifier.COMPACTION_COMMPLETENESS_GRANULARITY, "DAY");
    state.setProp(CompactionWatermarkChecker.TIME_FORMAT, "yyyy/MM/dd");

    FileSystemDataset dataset1201 = new SimpleFileSystemDataset(new Path("/dataset/2019/12/01"));
    FileSystemDataset dataset1130 = new SimpleFileSystemDataset(new Path("/dataset/2019/11/30"));
    FileSystemDataset datasetDash = new SimpleFileSystemDataset(new Path("/dataset/datepartition=2019-11-30"));

    // CASE: completeness is disabled
    state.setProp(CompactionAuditCountVerifier.COMPACTION_COMMPLETENESS_ENABLED, false);
    doVerifyDataset(new State(state), dataset1201, null, null);
    doVerifyDataset(new State(state), dataset1130, "1575187199999", null);
    doVerifyDataset(new State(state), datasetDash, null, null);

    // CASE: completeness is enabld
    state.setProp(CompactionAuditCountVerifier.COMPACTION_COMMPLETENESS_ENABLED, true);
    doVerifyDataset(new State(state), dataset1201, null, null);
    doVerifyDataset(new State(state), dataset1130, "1575187199999", "1575187199999");
    doVerifyDataset(new State(state), datasetDash, null, null);
  }

  private void doVerifyDataset(State state, FileSystemDataset dataset, String compactionWatermark, String completionAndCompactionWatermark) {
    CompactionWatermarkChecker checker = new CompactionWatermarkChecker(state);
    checker.verify(dataset);
    Assert.assertEquals(state.getProp(CompactionWatermarkChecker.COMPACTION_WATERMARK), compactionWatermark);
    Assert.assertEquals(state.getProp(CompactionWatermarkChecker.COMPLETION_COMPACTION_WATERMARK),
        completionAndCompactionWatermark);
  }
}
