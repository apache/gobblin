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

package org.apache.gobblin.writer;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.source.extractor.DefaultCheckpointableWatermark;
import org.apache.gobblin.source.extractor.extract.LongWatermark;


@Test
public class WatermarkTrackerTest {


  private void commits(WatermarkTracker watermarkTracker, String source, int... commit)
  {
    for (int oneCommit: commit) {
      watermarkTracker.committedWatermark(new DefaultCheckpointableWatermark(source, new LongWatermark(oneCommit)));
    }
  }

  @Test
  public void testSingleSource() {

    MultiWriterWatermarkTracker watermarkTracker = new MultiWriterWatermarkTracker();
    commits(watermarkTracker, "default", 0, 4, 5, 6);

    Assert.assertEquals(watermarkTracker.getCommittableWatermark("default").get().getSource(), "default");
    Assert.assertEquals(((LongWatermark) watermarkTracker.getCommittableWatermark("default")
        .get().getWatermark()).getValue(), 6L);
  }


  @Test
  public void testMultiSource() {
    MultiWriterWatermarkTracker watermarkTracker = new MultiWriterWatermarkTracker();
    commits(watermarkTracker, "default", 0, 4, 5, 6);
    commits(watermarkTracker, "other", 1, 3, 5, 7);

    Assert.assertEquals(watermarkTracker.getCommittableWatermark("default").get().getSource(), "default");
    Assert.assertEquals(((LongWatermark) watermarkTracker.getCommittableWatermark("default")
        .get().getWatermark()).getValue(), 6L);
    Assert.assertEquals(watermarkTracker.getCommittableWatermark("other").get().getSource(), "other");
    Assert.assertEquals(((LongWatermark) watermarkTracker.getCommittableWatermark("other")
        .get().getWatermark()).getValue(), 7L);

  }

}
