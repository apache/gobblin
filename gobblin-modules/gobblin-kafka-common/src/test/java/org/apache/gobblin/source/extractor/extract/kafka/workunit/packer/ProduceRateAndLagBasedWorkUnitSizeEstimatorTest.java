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
package org.apache.gobblin.source.extractor.extract.kafka.workunit.packer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaPartition;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaStreamingExtractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.io.GsonInterfaceAdapter;


public class ProduceRateAndLagBasedWorkUnitSizeEstimatorTest {
  private static final Gson GSON = GsonInterfaceAdapter.getGson(Object.class);
  private static final String TEST_TOPIC = "test";
  private static final long AVG_RECORD_SIZE = 1024L;
  private static final String BINPACKING_TIME_1 = "11/17/2019 23:10:00";
  private static final String BINPACKING_TIME_2 = "11/19/2019 08:00:00";

  private double[][] avgProduceRates = new double[7][24];
  private ProduceRateAndLagBasedWorkUnitSizeEstimator estimator;

  @BeforeClass
  public void setUp() {
    double rate = 1.0;
    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 24; j++) {
        if (i == 2) {
          avgProduceRates[i][j] = -1.0;
        } else {
          avgProduceRates[i][j] = rate * ProduceRateAndLagBasedWorkUnitSizeEstimator.ONE_MEGA_BYTE;
          rate++;
        }
      }
    }
  }

  @Test
  public void testCalcEstimatedSize() throws ParseException {
    SourceState sourceState = new SourceState();
    sourceState.setProp(ProduceRateAndLagBasedWorkUnitSizeEstimator.CATCHUP_SLA_IN_HOURS_KEY, 3);
    sourceState.setProp(ProduceRateAndLagBasedWorkUnitSizeEstimator.REPLANNING_INTERVAL_IN_HOURS_KEY, 3);
    sourceState.setProp(ProduceRateAndLagBasedWorkUnitSizeEstimator.PRODUCE_RATE_SCALING_FACTOR_KEY, 1);
    this.estimator = new ProduceRateAndLagBasedWorkUnitSizeEstimator(sourceState);
    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    format.setTimeZone(TimeZone.getDefault());

    //WorkUnit with no KafkaWatermark
    KafkaStreamingExtractor.KafkaWatermark watermark = null;
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PARTITION_WATERMARK, GSON.toJson(watermark));
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.DEFAULT_WORKUNIT_SIZE_KEY, 1.0);
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, Long.toString(6 * 3600 * 1024));
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PACKING_START_TIME_MILLIS, format.parse(BINPACKING_TIME_1).getTime());
    Assert.assertEquals(new Double(this.estimator.calcEstimatedSize(workUnit)).longValue(), 1L);

    //WorkUnit with Kafka watermark and previous avg produce rates
    watermark = new KafkaStreamingExtractor.KafkaWatermark(new KafkaPartition.Builder().withTopicName(TEST_TOPIC).withId(0).build(), new LongWatermark(0L));
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.MIN_WORKUNIT_SIZE_KEY, 2.0);
    watermark.setAvgRecordSize(AVG_RECORD_SIZE);
    watermark.setAvgProduceRates(avgProduceRates);
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PARTITION_WATERMARK, GSON.toJson(watermark));
    Assert.assertEquals(new Double(this.estimator.calcEstimatedSize(workUnit)).longValue(), 29L);

    //WorkUnit with Kafka watermark but no previous avg produce rates
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PACKING_START_TIME_MILLIS, format.parse(BINPACKING_TIME_2).getTime());
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.DEFAULT_WORKUNIT_SIZE_KEY, 2.0);
    Assert.assertEquals(new Double(this.estimator.calcEstimatedSize(workUnit)).longValue(), 4L);

    //Create a new workunit with minimum workunit size = 5.0
    workUnit = WorkUnit.createEmpty();
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PARTITION_WATERMARK, GSON.toJson(watermark));
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.DEFAULT_WORKUNIT_SIZE_KEY, 1.0);
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.MIN_WORKUNIT_SIZE_KEY, 5.0);
    workUnit.setProp(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY, Long.toString(6 * 3600 * 1024));
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.PACKING_START_TIME_MILLIS, format.parse(BINPACKING_TIME_2).getTime());
    workUnit.setProp(KafkaTopicGroupingWorkUnitPacker.DEFAULT_WORKUNIT_SIZE_KEY, 2.0);
    Assert.assertEquals(new Double(this.estimator.calcEstimatedSize(workUnit)).longValue(), 5L);
  }
}