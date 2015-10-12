/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.local.LocalJobLauncher;
import gobblin.runtime.mapreduce.MRJobLauncher;
import gobblin.util.JobConfigurationUtils;


/**
 * A factory class for building {@link JobLauncher} instances.
 *
 * @author ynli
 */
public class JobLauncherFactory {

  /**
   * Supported types of {@link JobLauncher}.
   */
  public enum JobLauncherType {
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
   * @param sysProps system configuration properties
   * @param jobProps job configuration properties
   * @return newly created {@link JobLauncher}
   */
  public static @Nonnull JobLauncher newJobLauncher(Properties sysProps, Properties jobProps) throws Exception {

    String launcherTypeValue =
        sysProps.getProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name());
    Optional<JobLauncherType> launcherType = Enums.getIfPresent(JobLauncherType.class, launcherTypeValue);

    if (launcherType.isPresent()) {
      switch (launcherType.get()) {
        case LOCAL:
          return new LocalJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
        case MAPREDUCE:
          return new MRJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
        default:
          throw new RuntimeException("Unsupported job launcher type: " + launcherType.get().name());
      }
    } else {
      @SuppressWarnings("unchecked")
      Class<? extends AbstractJobLauncher> launcherClass =
          (Class<? extends AbstractJobLauncher>) Class.forName(launcherTypeValue);
      return launcherClass.getDeclaredConstructor(Properties.class)
          .newInstance(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
    }
  }
}
