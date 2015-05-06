/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.io.Closeable;
import javax.annotation.Nullable;


/**
 * An interface for classes that launch a Gobblin job.
 *
 * <p>
 *   A {@link JobLauncher} is not supposed to be reused, i.e., each {@link JobLauncher}
 *   should only be used to launch a single job.
 * </p>
 *
 * @author ynli
 */
public interface JobLauncher extends Closeable {

  /**
   * Launch a Gobblin job.
   *
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @throws JobException if there is anything wrong launching and running the job
   */
  public void launchJob(@Nullable JobListener jobListener)
      throws JobException;

  /**
   * Cancel a Gobblin job.
   *
   * @param jobListener {@link JobListener} used for callback, can be <em>null</em> if no callback is needed.
   * @throws JobException if there is anything wrong cancelling the job
   */
  public void cancelJob(@Nullable JobListener jobListener)
      throws JobException;
}
