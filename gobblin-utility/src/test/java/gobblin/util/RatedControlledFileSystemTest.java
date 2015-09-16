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

package gobblin.util;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;
import com.google.common.math.DoubleMath;


/**
 * Unit tests for {@link RatedControlledFileSystem}.
 */
@Test(groups = { "gobblin.util" })
public class RatedControlledFileSystemTest {

  private static final Random RANDOM = new Random();

  private RateControlledFileSystem rateControlledFs;

  @BeforeClass
  public void setUp() throws IOException, ExecutionException {
    this.rateControlledFs = new RateControlledFileSystem(FileSystem.getLocal(new Configuration()), 20);
    this.rateControlledFs.startRateControl();
  }

  @Test
  public void testFsOperation() throws IOException, InterruptedException {
    Meter meter = new Meter();
    Path fakePath = new Path("fakePath");
    for (int i = 0; i < 1000; i++) {
      Assert.assertFalse(this.rateControlledFs.exists(fakePath));
      meter.mark();
      Thread.sleep((RANDOM.nextInt() & Integer.MAX_VALUE) % 10);
    }
    // Assert a fuzzy equal with 5% of tolerance
    Assert.assertTrue(DoubleMath.fuzzyEquals(meter.getMeanRate(), 20d, 20d * 0.05));
  }

  @AfterClass
  public void tearDown() throws IOException {
    this.rateControlledFs.close();
  }
}
