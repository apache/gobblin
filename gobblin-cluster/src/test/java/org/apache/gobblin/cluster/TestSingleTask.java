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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.testing.AssertWithBackoff;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;

import static org.apache.gobblin.cluster.SingleTask.MAX_RETRY_WAITING_FOR_INIT_KEY;


/**
 * Notes & Usage:
 * 0. This test could be used to reproduce task-execution issue in Gobblin-Cluster, within each container.
 * 1. The workunit is being created in {@link InMemoryWuFailedSingleTask}.
 * 2. When needed to reproduce certain errors, replace org.apache.gobblin.cluster.DummySource.DummyExtractor or
 * {@link DummySource} to plug in required logic.
 * 3. Some of this tests simulate the Helix scenarios where run() and cancel() coule be assigned to run in different threads.
 */
@Slf4j
public class TestSingleTask {

  private InMemorySingleTaskRunner createInMemoryTaskRunner()
      throws IOException {
    final File clusterWorkDirPath = Files.createTempDir();
    Path clusterConfigPath = Paths.get(clusterWorkDirPath.getAbsolutePath(), "clusterConf");
    Config config = ConfigFactory.empty().withValue(GobblinTaskRunner.CLUSTER_APP_WORK_DIR, ConfigValueFactory.fromAnyRef(clusterWorkDirPath.toString()));
    ConfigUtils configUtils = new ConfigUtils(new FileUtils());
    configUtils.saveConfigToFile(config, clusterConfigPath);

    final Path wuPath = Paths.get(clusterWorkDirPath.getAbsolutePath(), "_workunits/store/workunit.wu");

    InMemorySingleTaskRunner inMemorySingleTaskRunner =
        new InMemorySingleTaskRunner(clusterConfigPath.toString(), "testJob", wuPath.toString());
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

  // Normal sequence means, run is executed before cancel method.
  @Test
  public void testNormalSequence() throws Exception {
    InMemorySingleTaskRunner inMemorySingleTaskRunner = createInMemoryTaskRunner();

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
    final FutureTask<String> cancelTask = new FutureTask<String>(cancelRunnable, "cancelled");

    Runnable runRunnable = () -> {
      try {
        task.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    FutureTask<String> runTask = new FutureTask<String>(runRunnable, "completed");
    executor.submit(runTask);
    AssertWithBackoff.create().timeoutMs(2000).backoffFactor(1).assertTrue(new Predicate<Void>() {
                                                                             @Override
                                                                             public boolean apply(@Nullable Void input) {
                                                                               return task._taskAttempt != null;
                                                                             }
                                                                           }, "wait until task attempt available");

    // Simulate the process that signal happened first.
    executor.submit(cancelTask);

    AssertWithBackoff.create().timeoutMs(2000).backoffFactor(1).assertTrue(new Predicate<Void>() {
      @Override
      public boolean apply(@Nullable Void input) {
        return cancelTask.isDone();
      }
    }, "wait until task attempt available");
    Assert.assertEquals(cancelTask.get(), "cancelled");
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

    // The task.cancel() method has the logic to block on taskAttempt object to be initialized before calling
    // taskAttempt.shutdownTasks(). Here there has to be at least 2 threads running concurrently, the run() method
    // is meant to create the taskAttempt object so that the waiting thread (cancel thread) got unblocked after that.
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
