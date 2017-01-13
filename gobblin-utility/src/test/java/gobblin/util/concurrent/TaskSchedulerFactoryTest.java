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

package gobblin.util.concurrent;

import org.apache.commons.lang.StringUtils;
import org.hamcrest.Matchers;
import org.testng.Assert;
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
