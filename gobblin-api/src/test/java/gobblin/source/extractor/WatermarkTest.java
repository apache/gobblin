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

package gobblin.source.extractor;

import gobblin.configuration.WorkUnitState;
import gobblin.source.workunit.WorkUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link Watermark}, {@link WatermarkInterval}, and {@link WatermarkSerializerHelper}.
 */
@Test(groups = {"gobblin.source.extractor"})
public class WatermarkTest {

  @Test
  public void testWatermarkWorkUnitSerialization() {
    long lowWatermarkValue = 0;
    long expectedHighWatermarkValue = 100;

    TestWatermark lowWatermark = new TestWatermark();
    lowWatermark.setLongWatermark(lowWatermarkValue);

    TestWatermark expectedHighWatermark = new TestWatermark();
    expectedHighWatermark.setLongWatermark(expectedHighWatermarkValue);

    WatermarkInterval watermarkInterval = new WatermarkInterval(lowWatermark, expectedHighWatermark);
    WorkUnit workUnit = new WorkUnit(null, null, watermarkInterval);

    TestWatermark deserializedLowWatermark =
        WatermarkSerializerHelper.convertJsonToWatermark(workUnit.getLowWatermark(),
            TestWatermark.class);
    TestWatermark deserializedExpectedHighWatermark =
        WatermarkSerializerHelper.convertJsonToWatermark(workUnit.getExpectedHighWatermark(),
            TestWatermark.class);

    Assert.assertEquals(deserializedLowWatermark.getLongWatermark(), lowWatermarkValue);
    Assert.assertEquals(deserializedExpectedHighWatermark.getLongWatermark(), expectedHighWatermarkValue);
  }

  @Test
  public void testWatermarkWorkUnitStateSerialization() {
    long actualHighWatermarkValue = 50;

    TestWatermark actualHighWatermark = new TestWatermark();
    actualHighWatermark.setLongWatermark(actualHighWatermarkValue);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setActualHighWatermark(actualHighWatermark);

    TestWatermark deserializedActualHighWatermark =
        WatermarkSerializerHelper.convertJsonToWatermark(workUnitState.getActualHighWatermark(),
            TestWatermark.class);

    Assert.assertEquals(deserializedActualHighWatermark.getLongWatermark(), actualHighWatermarkValue);
  }
}
