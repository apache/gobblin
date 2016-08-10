/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.conversion.hive;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.SourceState;
import gobblin.data.management.conversion.hive.source.BackfillHiveSource;
import gobblin.source.extractor.extract.LongWatermark;


@Test(groups = {"gobblin.data.management.conversion"})
public class BackfillHiveSourceTest {

  @Test
  public void testNoWhitelist() throws Exception {

    BackfillHiveSource backfillHiveSource = new BackfillHiveSource();
    SourceState state = new SourceState();
    backfillHiveSource.initBackfillHiveSource(state);

    Partition sourcePartition = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Assert.assertTrue(backfillHiveSource.shouldCreateWorkunit(sourcePartition, new LongWatermark(0)));
  }

  @Test
  public void testWhitelist() throws Exception {

    BackfillHiveSource backfillHiveSource = new BackfillHiveSource();
    SourceState state = new SourceState();
    state.setProp(BackfillHiveSource.BACKFILL_SOURCE_PARTITION_WHITELIST_KEY,
        "service@logEvent@datepartition=2016-08-04-00,service@logEvent@datepartition=2016-08-05-00");
    backfillHiveSource.initBackfillHiveSource(state);

    Partition pass1 = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(pass1.getCompleteName()).thenReturn("service@logEvent@datepartition=2016-08-04-00");
    Partition pass2 = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(pass2.getCompleteName()).thenReturn("service@logEvent@datepartition=2016-08-05-00");

    Partition fail = Mockito.mock(Partition.class, Mockito.RETURNS_SMART_NULLS);
    Mockito.when(fail.getCompleteName()).thenReturn("service@logEvent@datepartition=2016-08-06-00");

    Assert.assertTrue(backfillHiveSource.shouldCreateWorkunit(pass1, new LongWatermark(0)));
    Assert.assertTrue(backfillHiveSource.shouldCreateWorkunit(pass2, new LongWatermark(0)));
    Assert.assertFalse(backfillHiveSource.shouldCreateWorkunit(fail, new LongWatermark(0)));
  }
}
