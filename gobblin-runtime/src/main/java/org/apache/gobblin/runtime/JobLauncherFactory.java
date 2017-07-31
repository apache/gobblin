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

import java.util.Properties;

import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.local.LocalJobLauncher;
import org.apache.gobblin.runtime.mapreduce.MRJobLauncher;
import org.apache.gobblin.util.JobConfigurationUtils;


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
    return newJobLauncher(sysProps, jobProps, null);
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
   * @param instanceBroker
   * @return newly created {@link JobLauncher}
   */
  public static @Nonnull JobLauncher newJobLauncher(Properties sysProps, Properties jobProps,
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker) throws Exception {

    String launcherTypeValue =
        sysProps.getProperty(ConfigurationKeys.JOB_LAUNCHER_TYPE_KEY, JobLauncherType.LOCAL.name());
    return newJobLauncher(sysProps, jobProps, launcherTypeValue, instanceBroker);
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
      String launcherTypeValue, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {
    Optional<JobLauncherType> launcherType = Enums.getIfPresent(JobLauncherType.class, launcherTypeValue);

    try {
      if (launcherType.isPresent()) {
        switch (launcherType.get()) {
          case LOCAL:
              return new LocalJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps), instanceBroker);
          case MAPREDUCE:
            return new MRJobLauncher(JobConfigurationUtils.combineSysAndJobProperties(sysProps, jobProps), instanceBroker);
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
