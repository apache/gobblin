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

package org.apache.gobblin.util.concurrent;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;


public abstract class TaskSchedulerTest {
  private final boolean scheduleFiresImmediately;

  public TaskSchedulerTest(boolean scheduleFiresImmediately) {
    this.scheduleFiresImmediately = scheduleFiresImmediately;
  }

  @Test
  public void testTaskRunsOnSchedule() throws IOException, InterruptedException {
    long startTime;
    long endTime;
    Task task = new Task("foo");
    try (TaskScheduler<String, Task> taskScheduler = getTaskScheduler("TaskSchedulerTest")) {
      startTime = System.currentTimeMillis();
      taskScheduler.schedule(task, 1, TimeUnit.SECONDS);
      task.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
      task.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
      task.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
      endTime = System.currentTimeMillis();
    }
    Assert.assertEquals(task.getCount(), 3);
    Assert.assertTrue(endTime - startTime >= (scheduleFiresImmediately ? 2000 : 3000));
  }

  @Test
  public void testCloseCancelsTasks() throws IOException, InterruptedException {
    Task task = new Task("foo");
    try (TaskScheduler<String, Task> taskScheduler = getTaskScheduler("TaskSchedulerTest")) {
      taskScheduler.schedule(task, 1, TimeUnit.SECONDS);
      task.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
    }
    task.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
    Assert.assertEquals(task.getCount(), 1);
  }

  @Test
  public void testScheduledTasksAreRetrievableAndCancellable() throws IOException, InterruptedException {
    Task task1 = new Task("foo");
    Task task2 = new Task("bar");
    try (TaskScheduler<String, Task> taskScheduler = getTaskScheduler("TaskSchedulerTest")) {
      taskScheduler.schedule(task1, 1, TimeUnit.SECONDS);
      taskScheduler.schedule(task2, 1, TimeUnit.SECONDS);
      task1.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
      task2.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
      Optional<Task> foo = taskScheduler.getScheduledTask("foo");
      Assert.assertTrue(foo.isPresent());
      Iterable<Task> all = taskScheduler.getScheduledTasks();
      Assert.assertEquals(Iterables.size(all), 2);
      taskScheduler.cancel(foo.get());
      foo = taskScheduler.getScheduledTask("foo");
      Assert.assertFalse(foo.isPresent());
      all = taskScheduler.getScheduledTasks();
      Assert.assertEquals(Iterables.size(all), 1);
      task2.getAutoResetEvent().waitOne(2, TimeUnit.SECONDS);
    }
    Assert.assertEquals(task1.getCount(), 1);
    Assert.assertEquals(task2.getCount(), 2);
  }

  protected abstract TaskScheduler<String, Task> getTaskScheduler(String name);

  protected class Task implements ScheduledTask<String> {
    private final String key;
    private final AutoResetEvent autoResetEvent;
    private int count = 0;

    public Task(String key) {
      this.key = key;
      this.autoResetEvent = new AutoResetEvent(false);;
    }

    @Override
    public String getKey() {
      return key;
    }

    public int getCount() {
      return count;
    }

    public AutoResetEvent getAutoResetEvent() {
      return autoResetEvent;
    }

    @Override
    public void runOneIteration() {
      count++;
      this.autoResetEvent.set();
    }
  }

}
