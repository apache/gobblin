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

package org.apache.gobblin.runtime;

import java.io.Closeable;
import javax.annotation.Nullable;

import org.apache.gobblin.runtime.listeners.JobListener;


/**
 * An interface for classes that launch a Gobblin job.
 *
 * <p>
 *   A {@link JobLauncher} is not supposed to be reused, i.e., each {@link JobLauncher}
 *   should only be used to launch a single job.
 * </p>
 *
 * @author Yinan Li
 */
public interface JobLauncher extends Closeable {

  /**
   * Launch a Gobblin job.
   *
   * <p>
   *   This method is synchronous, i.e., the caller will be blocked until the job finishes. The method
   *   {@link JobListener#onJobCompletion(JobContext)} of the given {@link JobListener} will be called at
   *   the end if no uncaught exceptions are thrown before the method gets called.
   * </p>
   *
   * @param jobListener a {@link JobListener} instance on which {@link JobListener#onJobCompletion(JobContext)}
   *                    is called at the end of this method if it is not {@code null}
   * @throws JobException if there is anything wrong launching and running the job
   */
  public void launchJob(@Nullable JobListener jobListener)
      throws JobException;

  /**
   * Cancel a Gobblin job.
   *
   * <p>
   *   This method is synchronous, i.e., the caller will be blocked until the cancellation is executed.
   *   The method {@link JobListener#onJobCancellation(JobContext)} of the given {@link JobListener} will
   *   be called at the end if the caller is not interrupted while being blocked. If a cancellation has
   *   already been requested, however, this method will return immediately.
   * </p>
   *
   * @param jobListener {@link JobListener} instance on which {@link JobListener#onJobCancellation(JobContext)}
   *                    is called at the end of this method if it is not {@code null}
   * @throws JobException if there is anything wrong cancelling the job
   */
  public void cancelJob(@Nullable JobListener jobListener)
      throws JobException;
}
