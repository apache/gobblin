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

package org.apache.gobblin.util;

import java.util.ArrayList;
import java.util.List;

import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.service.ServiceConfigKeys;


/** Tests for {@link WorkUnitSizeInfo}. */
public class WorkUnitSizeInfoTest {

  @Test
  public void testMultiWorkUnitSizeInfo() {
    List<WorkUnit> workUnits = new ArrayList<>();
    long totalSize = 0;
    for (int i = 1; i <= 20; i++) {
      long size = i * 100L;
      totalSize += size;
      WorkUnit wu = WorkUnit.createEmpty();
      wu.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, size);
      workUnits.add(wu);
    }
    MultiWorkUnit multiWorkUnit = MultiWorkUnit.createEmpty();
    multiWorkUnit.addWorkUnits(workUnits);

    double expectedMean = totalSize * 1.0 / workUnits.size();

    WorkUnitSizeInfo sizeInfo = WorkUnitSizeInfo.forWorkUnit(multiWorkUnit);

    Assert.assertEquals(sizeInfo.getNumConstituents(), 20);
    Assert.assertEquals(sizeInfo.getTotalSize(), totalSize);
    Assert.assertEquals(sizeInfo.getMedianSize(), 1100.0, 0.1);
    Assert.assertEquals(sizeInfo.getMeanSize(), expectedMean, 0.1);
    Assert.assertEquals(sizeInfo.getStddevSize(), 576.628, 0.1);
  }

  @Test
  public void testSingleWorkUnitSizeInfo() {
    WorkUnit workUnit = WorkUnit.createEmpty();
    workUnit.setProp(ServiceConfigKeys.WORK_UNIT_SIZE, 5432L);

    WorkUnitSizeInfo sizeInfo = WorkUnitSizeInfo.forWorkUnit(workUnit);

    Assert.assertEquals(sizeInfo.getNumConstituents(), 1);
    Assert.assertEquals(sizeInfo.getTotalSize(), 5432L);
    Assert.assertEquals(sizeInfo.getMedianSize(), 5432.0, 0.1);
    Assert.assertEquals(sizeInfo.getMeanSize(), 5432.0, 0.1);
    Assert.assertEquals(sizeInfo.getStddevSize(), 0.0, 0.1);
  }

  @Test
  public void testEncodeThenDecodeRoundTrip() {
    WorkUnitSizeInfo original = new WorkUnitSizeInfo(20, 21000L, 1100.0, 1050.0, 576.628);
    String encoded = original.encode();

    Assert.assertEquals(encoded, "n=20-total=21000-median=1100.000-mean=1050.000-stddev=576.628");

    Optional<WorkUnitSizeInfo> optDecoded = WorkUnitSizeInfo.decode(encoded);
    Assert.assertTrue(optDecoded.isPresent());
    WorkUnitSizeInfo decoded = optDecoded.get();
    Assert.assertEquals(decoded.getNumConstituents(), original.getNumConstituents());
    Assert.assertEquals(decoded.getTotalSize(), original.getTotalSize());
    Assert.assertEquals(decoded.getMedianSize(), original.getMedianSize(), 0.1);
    Assert.assertEquals(decoded.getMeanSize(), original.getMeanSize(), 0.1);
    Assert.assertEquals(decoded.getStddevSize(), original.getStddevSize(), 0.1);
  }

  @Test
  public void testDecodeFromIntFormattedDoubles() {
    String encoded = "n=20-total=12345-median=1111-mean=617-stddev=543";

    Optional<WorkUnitSizeInfo> optDecoded = WorkUnitSizeInfo.decode(encoded);
    Assert.assertTrue(optDecoded.isPresent());
    WorkUnitSizeInfo decoded = optDecoded.get();
    Assert.assertEquals(decoded.getNumConstituents(), 20);
    Assert.assertEquals(decoded.getTotalSize(), 12345L);
    Assert.assertEquals(decoded.getMedianSize(), 1111.0);
    Assert.assertEquals(decoded.getMeanSize(), 617.0);
    Assert.assertEquals(decoded.getStddevSize(), 543.0);
  }
}
