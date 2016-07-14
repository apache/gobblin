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
package gobblin.data.management.conversion.hive.validation;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for Hive validation job.
 *
 * @author Abhishek Tiwari
 */
@Test(groups = {"gobblin.data.management.conversion"})
public class ValidationJobTest {

  private long currentTime;
  private long updateTime;
  private long maxLookbackTime;
  private long skipRecentThanDays;

  @BeforeClass
  public void setup() throws Exception {
    this.currentTime = System.currentTimeMillis();
    this.updateTime = new DateTime(currentTime).minusDays(35).getMillis();
    this.maxLookbackTime = new DateTime(currentTime).minusDays(3).getMillis();
    this.skipRecentThanDays = new DateTime(currentTime).minusDays(1).getMillis();
  }

  @Test
  public void testShouldValidateBeforeMaxLookBack() throws Exception {
    boolean shouldValidate = ValidationJob.shouldValidate(this.updateTime, this.maxLookbackTime,
        this.skipRecentThanDays);

    Assert.assertEquals(shouldValidate, false, "Should not validate older than maxLookbackDays");
  }

  @Test
  public void testShouldValidateAfterSkipRecentThanDays() throws Exception {
    boolean shouldValidate = ValidationJob.shouldValidate(this.currentTime, this.maxLookbackTime,
        this.skipRecentThanDays);

    Assert.assertEquals(shouldValidate, false, "Should not validate newer than skipRecentThanDays");
  }

  @Test
  public void testShouldValidateWithinValidationWindow() throws Exception {
    long updateTime = new DateTime(this.currentTime).minusDays(2).getMillis();

    boolean shouldValidate = ValidationJob.shouldValidate(updateTime, this.maxLookbackTime,
        this.skipRecentThanDays);
    Assert.assertEquals(shouldValidate, true, "Should validate when update time is between "
        + "maxLookbackDays and skipRecentThanDays");
  }
}
