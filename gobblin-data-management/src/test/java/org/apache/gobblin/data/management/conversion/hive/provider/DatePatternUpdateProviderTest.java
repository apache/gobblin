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
package org.apache.gobblin.data.management.conversion.hive.provider;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DatePatternUpdateProviderTest {

  private static final long EPOCH_2016_02_02 = 1454400000000l;
  private static final long EPOCH_2016_02_02_10 = 1454436000000l;

  @Test
  public void testDaily() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/daily/2016/02/02");
    Assert.assertEquals(updateProvider.getUpdateTime(mockPartition), EPOCH_2016_02_02);
  }

  @Test
  public void testDailyLate() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/daily_late/2016/02/02");
    Assert.assertEquals(updateProvider.getUpdateTime(mockPartition), EPOCH_2016_02_02);
  }

  @Test
  public void testHourly() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/hourly/2016/02/02/10");
    Assert.assertEquals(updateProvider.getUpdateTime(mockPartition), EPOCH_2016_02_02_10);
  }

  @Test
  public void testHourlyLate() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/hourly_late/2016/02/02/10");
    Assert.assertEquals(updateProvider.getUpdateTime(mockPartition), EPOCH_2016_02_02_10);
  }

  @Test
  public void testHourlyDeduped() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/hourly_deduped/2016/02/02/10");
    Assert.assertEquals(updateProvider.getUpdateTime(mockPartition), EPOCH_2016_02_02_10);
  }

  @Test(expectedExceptions = UpdateNotFoundException.class)
  public void testHourlyInvalid() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/hourly/2016/02/abc/10");
    updateProvider.getUpdateTime(mockPartition);
  }

  @Test(expectedExceptions = UpdateNotFoundException.class)
  public void testNoMatchingPattern() throws Exception {
    HiveUnitUpdateProvider updateProvider = new DatePatternUpdateProvider();
    Partition mockPartition = createMockPartitionWithLocation("/data/TestEvent/2016/02/02/10");
    updateProvider.getUpdateTime(mockPartition);
  }

  public static Partition createMockPartitionWithLocation(String location) {
    Partition mockPartition = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    org.apache.hadoop.hive.metastore.api.Partition mockTPartition =
        Mockito.mock(org.apache.hadoop.hive.metastore.api.Partition.class, Mockito.RETURNS_SMART_NULLS);
    StorageDescriptor mockSd = Mockito.mock(StorageDescriptor.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(mockSd.getLocation()).thenReturn(location);
    Mockito.when(mockTPartition.getSd()).thenReturn(mockSd);
    Mockito.when(mockPartition.getTPartition()).thenReturn(mockTPartition);
    return mockPartition;
  }
}
