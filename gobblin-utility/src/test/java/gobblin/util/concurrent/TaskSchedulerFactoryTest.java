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

package gobblin.util.concurrent;

import org.apache.commons.lang.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;


public class TaskSchedulerFactoryTest {
  @Test
  public void testGet() {
    TaskScheduler<Object, ScheduledTask<Object>> taskScheduler = TaskSchedulerFactory.get("", Optional.<String>absent());
    Assert.assertTrue(Matchers.instanceOf(ScheduledExecutorServiceTaskScheduler.class).matches(taskScheduler));
    taskScheduler = TaskSchedulerFactory.get(StringUtils.EMPTY, Optional.<String>absent());
    Assert.assertTrue(Matchers.instanceOf(ScheduledExecutorServiceTaskScheduler.class).matches(taskScheduler));
    taskScheduler = TaskSchedulerFactory.get("ScheduledExecutorService", Optional.<String>absent());
    Assert.assertTrue(Matchers.instanceOf(ScheduledExecutorServiceTaskScheduler.class).matches(taskScheduler));
    taskScheduler = TaskSchedulerFactory.get("HashedWheelTimer", Optional.<String>absent());
    Assert.assertTrue(Matchers.instanceOf(HashedWheelTimerTaskScheduler.class).matches(taskScheduler));
  }
}
