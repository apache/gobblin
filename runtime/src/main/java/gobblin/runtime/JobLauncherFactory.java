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

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.local.LocalJobLauncher;
import gobblin.runtime.mapreduce.MRJobLauncher;


/**
 * A factory class for building {@link JobLauncher} instances.
 *
 * @author ynli
 */
public class JobLauncherFactory {

  /**
   * Supported types of {@link JobLauncher}.
   */
  enum JobLauncherType {
    LOCAL,
    MAPREDUCE,
    YARN
  }

  /**
   * Create a new {@link JobLauncher}.
   *
   * @param properties Framework configuration properties
   * @return Newly created {@link JobLauncher}
   */
  public static JobLauncher newJobLauncher(Properties properties)
      throws Exception {
    JobLauncherType launcherType = JobLauncherType
        .valueOf(properties.getProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name()));
    switch (launcherType) {
      case LOCAL:
        return new LocalJobLauncher(properties);
      case MAPREDUCE:
        return new MRJobLauncher(properties);
      default:
        throw new RuntimeException("Unsupported job launcher type: " + launcherType.name());
    }
  }
}
