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

import java.util.Properties;


/**
 * An interface for classes that launch a Gobblin job.
 *
 * @author ynli
 */
public interface JobLauncher {

  /**
   * Launch a Gobblin job.
   *
   * @param jobProps Job configuration properties
   * @param jobListener {@link JobListener} used for callback,
   *                    can be <em>null</em> if no callback is needed.
   * @throws JobException
   */
  public void launchJob(Properties jobProps, JobListener jobListener)
      throws JobException;

  /**
   * Cancel a Gobblin job.
   *
   * @param jobProps Job configuration properties
   * @throws JobException
   */
  public void cancelJob(Properties jobProps)
      throws JobException;
}
