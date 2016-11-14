/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
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
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.local.LocalJobLauncher;
import gobblin.runtime.mapreduce.MRJobLauncher;
import gobblin.util.JobConfigurationUtils;


/**
 * A factory class for building {@link JobLauncher} instances.
 *
 * @author Yinan Li
 */
@Slf4j
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
    return newJobLauncher(sysProps, jobProps, launcherTypeValue);
  }

  /**
   * Creates a new instance for a JobLauncher with a given type
   * @param sysProps          the system/environment properties
   * @param jobProps          the job properties
   * @param launcherTypeValue the type of the launcher; either a {@link JobLauncherType} value or
   *        the name of the class that extends {@link AbstractJobLauncher} and has a constructor
   *        that has a single Properties parameter..
   * @return the JobLauncher instance
   * @throws RuntimeException if the instantiation fails
   */
  public static JobLauncher newJobLauncher(Properties sysProps, Properties jobProps,
      String launcherTypeValue) {
    Optional<JobLauncherType> launcherType = Enums.getIfPresent(JobLauncherType.class, launcherTypeValue);

    try {
      if (launcherType.isPresent()) {
        switch (launcherType.get()) {
          case LOCAL:
              return new LocalJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
          case MAPREDUCE:
            return new MRJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
          default:
            throw new RuntimeException("Unsupported job launcher type: " + launcherType.get().name());
        }
      }

      @SuppressWarnings("unchecked")
      Class<? extends AbstractJobLauncher> launcherClass =
          (Class<? extends AbstractJobLauncher>) Class.forName(launcherTypeValue);
      return launcherClass.getDeclaredConstructor(Properties.class)
          .newInstance(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create job launcher: " + e, e);
    }
  }
}
