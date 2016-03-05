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

import com.google.common.collect.ImmutableMap;
import gobblin.util.options.annotations.Checked;
import gobblin.util.options.annotations.ClassInstantiationMap;
import gobblin.util.options.annotations.UserOption;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nonnull;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import gobblin.runtime.local.LocalJobLauncher;
import gobblin.runtime.mapreduce.MRJobLauncher;
import gobblin.util.JobConfigurationUtils;


/**
 * A factory class for building {@link JobLauncher} instances.
 *
 * @author Yinan Li
 */
@Checked(shortName = "Job Launcher Factory")
public class JobLauncherFactory {

  // Job launcher type
  @UserOption(required = true, values = JobLauncherFactory.JobLauncherType.class, shortName = "Launcher Type")
  public static final String JOB_LAUNCHER_TYPE_KEY = "launcher.type";

  /**
   * Supported types of {@link JobLauncher}.
   */
  public enum JobLauncherType {
    LOCAL,
    MAPREDUCE,
    YARN;

    @ClassInstantiationMap
    public static final Map<JobLauncherType, Class<?>> jobLauncherClass = ImmutableMap.<JobLauncherType, Class<?>>of(
        LOCAL, LocalJobLauncher.class, MAPREDUCE, MRJobLauncher.class);
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
        sysProps.getProperty(JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name());
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
