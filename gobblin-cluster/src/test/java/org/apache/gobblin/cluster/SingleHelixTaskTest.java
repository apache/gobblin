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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.helix.task.TaskResult;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SingleHelixTaskTest {
  @Test
  public void testRun()
      throws IOException, InterruptedException {
    final SingleTaskLauncher mockLauncher = mock(SingleTaskLauncher.class);
    final Process mockProcess = mock(Process.class);
    when(mockLauncher.launch(any(), any())).thenReturn(mockProcess);
    final ImmutableMap<String, String> configMap = ImmutableMap
        .of("job.name", "testJob", "job.id", "1", "gobblin.cluster.work.unit.file.path",
            "work-unit.wu");

    final SingleHelixTask task = new SingleHelixTask(mockLauncher, configMap);
    final TaskResult result = task.run();

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.COMPLETED);
    final Path expectedPath = Paths.get("work-unit.wu");
    verify(mockLauncher).launch("1", expectedPath);
    verify(mockProcess).waitFor();
  }

  @Test
  public void interruptedProcessShouldResultInCanceledStatus()
      throws IOException, InterruptedException {
    final TaskResult result = createTaskWithException(new InterruptedException());

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.CANCELED);
  }

  @Test
  public void NonInterruptedExceptionShouldResultInFailedStatus()
      throws IOException, InterruptedException {
    final TaskResult result = createTaskWithException(new RuntimeException());

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.FAILED);
  }

  private TaskResult createTaskWithException(final Exception exceptionToThrow)
      throws InterruptedException, IOException {
    final SingleTaskLauncher mockLauncher = mock(SingleTaskLauncher.class);
    final Process mockProcess = mock(Process.class);
    when(mockLauncher.launch(any(), any())).thenReturn(mockProcess);
    when(mockProcess.waitFor()).thenThrow(exceptionToThrow);
    final ImmutableMap<String, String> configMap =
        ImmutableMap.of("gobblin.cluster.work.unit.file.path", "work-unit.wu");

    final SingleHelixTask task = new SingleHelixTask(mockLauncher, configMap);
    return task.run();
  }

  @Test
  public void testCancel()
      throws IOException {
    final SingleTaskLauncher mockLauncher = mock(SingleTaskLauncher.class);
    final Process mockProcess = mock(Process.class);
    when(mockLauncher.launch(any(), any())).thenReturn(mockProcess);
    final ImmutableMap<String, String> configMap =
        ImmutableMap.of("gobblin.cluster.work.unit.file.path", "work-unit.wu");

    final SingleHelixTask task = new SingleHelixTask(mockLauncher, configMap);
    task.cancel();
    verify(mockProcess).destroyForcibly();
  }
}
