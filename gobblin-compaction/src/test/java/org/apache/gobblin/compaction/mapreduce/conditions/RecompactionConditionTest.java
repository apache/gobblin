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

package org.apache.gobblin.compaction.mapreduce.conditions;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;


import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.base.Optional;
import org.apache.gobblin.compaction.conditions.RecompactionCondition;
import org.apache.gobblin.compaction.conditions.RecompactionCombineCondition;
import org.apache.gobblin.compaction.conditions.RecompactionConditionBasedOnDuration;
import org.apache.gobblin.compaction.conditions.RecompactionConditionBasedOnFileCount;
import org.apache.gobblin.compaction.conditions.RecompactionConditionBasedOnRatio;
import org.apache.gobblin.compaction.conditions.RecompactionConditionFactory;
import org.apache.gobblin.compaction.dataset.Dataset;
import org.apache.gobblin.compaction.dataset.DatasetHelper;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test class for {@link org.apache.gobblin.compaction.conditions.RecompactionCondition}.
 */
@Test(groups = {"gobblin.compaction.mapreduce.conditions"})
public class RecompactionConditionTest {
  private Path inputPath      = new Path ("/tmp/input");
  private Path inputLatePath  = new Path ("/tmp/input_late");
  private Path outputPath     = new Path ("/tmp/output");
  private Path outputLatePath = new Path ("/tmp/output_late");
  private Path tmpPath        = new Path ("/tmp/output_tmp");
  private Dataset dataset;
  private Logger LOG = LoggerFactory.getLogger(RecompactionConditionTest.class);
  public DateTime getCurrentTime() {
    DateTimeZone timeZone = DateTimeZone.forID(MRCompactor.DEFAULT_COMPACTION_TIMEZONE);
    DateTime currentTime = new DateTime(timeZone);
    return currentTime;
  }

  @BeforeClass
  public void setUp() throws IOException {
    dataset =
        new Dataset.Builder().withPriority(1.0)
            .withDatasetName("Identity/MemberAccount")
            .withInputPath(inputPath)
            .withInputLatePath(inputLatePath)
            .withOutputPath(outputPath)
            .withOutputLatePath(outputLatePath)
            .withOutputTmpPath(tmpPath).build();

    dataset.setJobProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_DURATION, MRCompactor.DEFAULT_COMPACTION_LATEDATA_THRESHOLD_DURATION);
    dataset.setJobProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FILE_NUM, 3);
    dataset.setJobProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_FOR_RECOMPACT_PER_DATASET, "Identity.*,B.*:0.2; C.*,D.*:0.3");
    dataset.setJobProp(MRCompactor.COMPACTION_LATEDATA_THRESHOLD_DURATION, "12h");
  }

  @Test
  public void testRecompactionConditionBasedOnFileCount() {
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      fs.delete(outputLatePath, true);
      fs.mkdirs(outputLatePath);
      RecompactionConditionFactory factory = new RecompactionConditionBasedOnFileCount.Factory();
      RecompactionCondition conditionBasedOnFileCount= factory.createRecompactionCondition(dataset);
      DatasetHelper helper = new DatasetHelper(dataset, fs, Lists.newArrayList("avro"));

      fs.createNewFile(new Path(outputLatePath, new Path ("1.avro")));
      fs.createNewFile(new Path(outputLatePath, new Path ("2.avro")));
      Assert.assertEquals(conditionBasedOnFileCount.isRecompactionNeeded(helper), false);

      fs.createNewFile(new Path(outputLatePath, new Path ("3.avro")));
      Assert.assertEquals(conditionBasedOnFileCount.isRecompactionNeeded(helper), true);

      fs.delete(outputLatePath, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testRecompactionConditionBasedOnRatio() {
    RecompactionConditionFactory factory = new RecompactionConditionBasedOnRatio.Factory();
    RecompactionCondition conditionBasedOnRatio = factory.createRecompactionCondition(dataset);
    DatasetHelper helper = mock(DatasetHelper.class);

    when(helper.getLateOutputRecordCount()).thenReturn(6L);
    when(helper.getOutputRecordCount()).thenReturn(94L);
    Assert.assertEquals(conditionBasedOnRatio.isRecompactionNeeded(helper), false);

    when(helper.getLateOutputRecordCount()).thenReturn(21L);
    when(helper.getOutputRecordCount()).thenReturn(79L);
    Assert.assertEquals(conditionBasedOnRatio.isRecompactionNeeded(helper), true);

  }

  @Test
  public void testRecompactionConditionBasedOnDuration() {
    RecompactionConditionFactory factory = new RecompactionConditionBasedOnDuration.Factory();
    RecompactionCondition conditionBasedOnDuration = factory.createRecompactionCondition(dataset);
    DatasetHelper helper = mock (DatasetHelper.class);
    when(helper.getDataset()).thenReturn(dataset);
    PeriodFormatter periodFormatter = new PeriodFormatterBuilder().appendMonths().appendSuffix("m").appendDays().appendSuffix("d").appendHours()
        .appendSuffix("h").appendMinutes().appendSuffix("min").toFormatter();
    DateTime currentTime = getCurrentTime();

    Period period_A = periodFormatter.parsePeriod("11h59min");
    DateTime earliest_A = currentTime.minus(period_A);
    when(helper.getEarliestLateFileModificationTime()).thenReturn(Optional.of(earliest_A));
    when(helper.getCurrentTime()).thenReturn(currentTime);
    Assert.assertEquals(conditionBasedOnDuration.isRecompactionNeeded(helper), false);

    Period period_B = periodFormatter.parsePeriod("12h01min");
    DateTime earliest_B = currentTime.minus(period_B);
    when(helper.getEarliestLateFileModificationTime()).thenReturn(Optional.of(earliest_B));
    when(helper.getCurrentTime()).thenReturn(currentTime);
    Assert.assertEquals(conditionBasedOnDuration.isRecompactionNeeded(helper), true);
  }

  @Test
  public void testRecompactionCombineCondition() {

    DatasetHelper helper = mock (DatasetHelper.class);
    RecompactionCondition cond1 = mock (RecompactionConditionBasedOnRatio.class);
    RecompactionCondition cond2= mock (RecompactionConditionBasedOnFileCount.class);
    RecompactionCondition cond3 = mock (RecompactionConditionBasedOnDuration.class);

    RecompactionCombineCondition combineConditionOr =  new RecompactionCombineCondition(Arrays.asList(cond1,cond2,cond3),
        RecompactionCombineCondition.CombineOperation.OR);

    when(cond1.isRecompactionNeeded(helper)).thenReturn(false);
    when(cond2.isRecompactionNeeded(helper)).thenReturn(false);
    when(cond3.isRecompactionNeeded(helper)).thenReturn(false);
    Assert.assertEquals(combineConditionOr.isRecompactionNeeded(helper), false);

    when(cond1.isRecompactionNeeded(helper)).thenReturn(false);
    when(cond2.isRecompactionNeeded(helper)).thenReturn(true);
    when(cond3.isRecompactionNeeded(helper)).thenReturn(false);
    Assert.assertEquals(combineConditionOr.isRecompactionNeeded(helper), true);

    RecompactionCombineCondition combineConditionAnd =  new RecompactionCombineCondition(Arrays.asList(cond1,cond2,cond3),
        RecompactionCombineCondition.CombineOperation.AND);
    when(cond1.isRecompactionNeeded(helper)).thenReturn(true);
    when(cond2.isRecompactionNeeded(helper)).thenReturn(true);
    when(cond3.isRecompactionNeeded(helper)).thenReturn(false);
    Assert.assertEquals(combineConditionAnd.isRecompactionNeeded(helper), false);

    when(cond1.isRecompactionNeeded(helper)).thenReturn(true);
    when(cond2.isRecompactionNeeded(helper)).thenReturn(true);
    when(cond3.isRecompactionNeeded(helper)).thenReturn(true);
    Assert.assertEquals(combineConditionAnd.isRecompactionNeeded(helper), true);
  }
}