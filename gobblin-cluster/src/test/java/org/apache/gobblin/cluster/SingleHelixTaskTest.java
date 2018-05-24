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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SingleHelixTaskTest {
  private static final String WORK_UNIT_FILE_PATH = "work-unit.wu";
  private static final String JOB_ID = "1";
  private Process mockProcess;
  private SingleTaskLauncher mockLauncher;
  private SingleHelixTask task;

  @BeforeMethod
  public void setUp() {
    this.mockLauncher = mock(SingleTaskLauncher.class);
    this.mockProcess = mock(Process.class);
  }

  @Test
  public void successTaskProcessShouldResultInCompletedStatus()
      throws IOException, InterruptedException {
    when(this.mockProcess.waitFor()).thenReturn(0);
    final TaskResult result = createAndRunTask();

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.COMPLETED);
    final Path expectedPath = Paths.get(WORK_UNIT_FILE_PATH);
    verify(this.mockLauncher).launch(JOB_ID, expectedPath);
    verify(this.mockProcess).waitFor();
  }

  @Test
  public void failedTaskProcessShouldResultInFailedStatus()
      throws IOException, InterruptedException {
    when(this.mockProcess.waitFor()).thenReturn(1);

    final TaskResult result = createAndRunTask();

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.FATAL_FAILED);
  }

  @Test
  public void NonInterruptedExceptionShouldResultInFailedStatus()
      throws IOException, InterruptedException {
    when(this.mockProcess.waitFor()).thenThrow(new RuntimeException());

    final TaskResult result = createAndRunTask();

    assertThat(result.getStatus()).isEqualTo(TaskResult.Status.FAILED);
  }

  @Test
  public void testCancel()
      throws IOException {
    createAndRunTask();
    this.task.cancel();

    verify(this.mockProcess).destroyForcibly();
  }

  private TaskResult createAndRunTask()
      throws IOException {
    when(this.mockLauncher.launch(any(), any())).thenReturn(this.mockProcess);
    final ImmutableMap<String, String> configMap = ImmutableMap
        .of("job.name", "testJob", "job.id", JOB_ID, "gobblin.cluster.work.unit.file.path",
            WORK_UNIT_FILE_PATH);

    this.task = new SingleHelixTask(this.mockLauncher, configMap);
    return this.task.run();
  }
}
