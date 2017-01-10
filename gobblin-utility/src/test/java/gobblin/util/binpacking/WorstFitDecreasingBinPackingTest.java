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

package gobblin.util.binpacking;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.source.workunit.Extract;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitWeighter;


public class WorstFitDecreasingBinPackingTest {

  public static final String WEIGHT = "weight";

  WorkUnitWeighter weighter = new FieldWeighter(WEIGHT);

  @Test
  public void testBasicPacking() throws Exception {

    List<WorkUnit> workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(10));
    List<WorkUnit> multiWorkUnits = new WorstFitDecreasingBinPacking(20).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 2);

    workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(20));
    multiWorkUnits = new WorstFitDecreasingBinPacking(20).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 2);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(1)).getWorkUnits().size(), 1);

    workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(20));
    multiWorkUnits = new WorstFitDecreasingBinPacking(20).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 2);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(1)).getWorkUnits().size(), 2);

  }

  @Test
  public void testLargeWorkUnits() throws Exception {
    // Accept even large work units that don't fit in a single bucket
    List<WorkUnit> workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(30));
    List<WorkUnit> multiWorkUnits = new WorstFitDecreasingBinPacking(20).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 2);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(1)).getWorkUnits().size(), 1);
  }

  @Test
  public void testOneLargeUnitManySmallUnits() throws Exception {
    // Check that a large work unit doesn't prevent small work units from being packed together
    // (this was an issue in a previous implementation of the algorithm)
    List<WorkUnit> workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(10),
        getWorkUnitWithWeight(10000));
    List<WorkUnit> multiWorkUnits = new WorstFitDecreasingBinPacking(50).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 2);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 3);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(1)).getWorkUnits().size(), 1);
  }

  @Test
  public void testMaxSizeZero() throws Exception {
    // If maxSize is 0, one work unit per bin
    List<WorkUnit> workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(1),
        getWorkUnitWithWeight(1));
    List<WorkUnit> multiWorkUnits = new WorstFitDecreasingBinPacking(0).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 2);
    Assert.assertEquals(workUnitList, multiWorkUnits);
  }

  @Test
  public void testOverflows() throws Exception {
    // Test overflows
    List<WorkUnit> workUnitList = Lists.newArrayList(
        getWorkUnitWithWeight(Long.MAX_VALUE),
        getWorkUnitWithWeight(Long.MAX_VALUE),
        getWorkUnitWithWeight(10));
    List<WorkUnit> multiWorkUnits = new WorstFitDecreasingBinPacking(100).pack(workUnitList, weighter);
    Assert.assertEquals(multiWorkUnits.size(), 3);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(0)).getWorkUnits().size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(1)).getWorkUnits().size(), 1);
    Assert.assertEquals(((MultiWorkUnit) multiWorkUnits.get(2)).getWorkUnits().size(), 1);
  }

  public WorkUnit getWorkUnitWithWeight(long weight) {
    WorkUnit workUnit = new WorkUnit(new Extract(Extract.TableType.APPEND_ONLY, "", ""));
    workUnit.setProp(WEIGHT, Long.toString(weight));
    return workUnit;
  }

}
