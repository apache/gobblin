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

package org.apache.gobblin.cluster;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.gobblin.testing.AssertWithBackoff;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;

import javax.annotation.Nullable;

import static org.apache.gobblin.cluster.SingleTask.MAX_RETRY_WAITING_FOR_INIT_KEY;


/**
 * Notes & Usage:
 * 0. This test could be used to reproduce task-execution issue in Gobblin-Cluster, within each container.
 * 1. The workunit is being created in {@link InMemoryWuFailedSingleTask}.
 * 2. When needed to reproduce certain errors, replace org.apache.gobblin.cluster.DummySource.DummyExtractor or
 * {@link DummySource} to plug in required logic.
 */
public class TestSingleTask {

  private InMemorySingleTaskRunner createInMemoryTaskRunner() {
    final String clusterConfigPath = "clusterConf";
    final String wuPath = "_workunits/store/workunit.wu";
    String clusterConfPath = this.getClass().getClassLoader().getResource(clusterConfigPath).getPath();

    InMemorySingleTaskRunner inMemorySingleTaskRunner = new InMemorySingleTaskRunner(clusterConfPath, "testJob",
        this.getClass().getClassLoader().getResource(wuPath).getPath());

    return inMemorySingleTaskRunner;
  }

  /**
   * An in-memory {@link SingleTask} runner that could be used to simulate how it works in Gobblin-Cluster.
   * For this example method, it fail the execution by missing certain configuration on purpose, catch the exception and
   * re-run it again.
   */
  @Test
  public void testSingleTaskRerunAfterFailure()
      throws Exception {
    SingleTaskRunner inMemorySingleTaskRunner = createInMemoryTaskRunner();
    try {
      inMemorySingleTaskRunner.run(true);
    } catch (Exception e) {
      inMemorySingleTaskRunner.run();
    }

    Assert.assertTrue(true);
  }

  @Test
  public void testTaskCancelBeforeRunFailure() throws Exception {
    InMemorySingleTaskRunner inMemorySingleTaskRunner = createInMemoryTaskRunner();
    inMemorySingleTaskRunner.initClusterSingleTask(false);

    // Directly calling cancel without initializing taskAttempt, it will timeout until reaching illegal state defined
    // in SingleTask.
    try {
      inMemorySingleTaskRunner.task.cancel();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalStateException);
      Assert.assertTrue(e.getMessage().contains("Failed to initialize"));
    }
  }

  @Test
  public void testTaskCancelBeforeRun()
      throws Exception {
    final InMemorySingleTaskRunner inMemorySingleTaskRunner = createInMemoryTaskRunner();

    // Place cancellation into infinite wait while having another thread initialize the taskAttempt.
    // Reset task and set the retry to be infinite large.
    inMemorySingleTaskRunner
        .setInjectedConfig(ConfigFactory.parseMap(ImmutableMap.of(MAX_RETRY_WAITING_FOR_INIT_KEY, Integer.MAX_VALUE)));
    inMemorySingleTaskRunner.startServices();
    inMemorySingleTaskRunner.initClusterSingleTask(false);
    final SingleTask task = inMemorySingleTaskRunner.task;
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Runnable cancelRunnable = () -> {
      try {
        task.cancel();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
    FutureTask<String> cancelTask = new FutureTask<String>(cancelRunnable, "cancelled");
    executor.submit(cancelTask);

    Runnable runRunnable = () -> {
      try {
        task.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    FutureTask<String> runTask = new FutureTask<String>(runRunnable, "completed");
    executor.submit(runTask);

    AssertWithBackoff assertWithBackoff = AssertWithBackoff.create().backoffFactor(1).maxSleepMs(1000).timeoutMs(500000);
    assertWithBackoff.assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return runTask.isDone();
      }
    }, "waiting for future to complete");
    Assert.assertEquals(runTask.get(), "completed");
    Assert.assertTrue(cancelTask.isDone());
    Assert.assertEquals(cancelTask.get(), "cancelled");
  }
}
