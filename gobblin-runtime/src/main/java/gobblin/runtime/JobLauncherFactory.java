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
import javax.annotation.Nonnull;

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
   * <p>
   *   This method will never return a {@code null}.
   * </p>
   *
   * @param properties framework configuration properties
   * @param jobProps job configuration properties
   * @return newly created {@link JobLauncher}
   */
  public static @Nonnull JobLauncher newJobLauncher(Properties properties, Properties jobProps)
      throws Exception {
    JobLauncherType launcherType = JobLauncherType
        .valueOf(properties.getProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name()));
    switch (launcherType) {
      case LOCAL:
        return new LocalJobLauncher(properties, jobProps);
      case MAPREDUCE:
        return new MRJobLauncher(properties, jobProps);
      default:
        throw new RuntimeException("Unsupported job launcher type: " + launcherType.name());
    }
  }
}
