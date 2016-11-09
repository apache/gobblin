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

package gobblin.runtime.locks;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import com.google.common.base.Preconditions;
import gobblin.configuration.ConfigurationKeys;
import org.apache.commons.lang3.reflect.ConstructorUtils;


/**
 * The factory used to create instances of {@link JobLock}.
 *
 * @author joelbaranick
 */
public class JobLockFactory {
  private JobLockFactory() {
  }

  /**
   * Gets an instance of {@link JobLock}.
   *
   * @param properties the properties used to determine which instance of {@link JobLock} to create and the
   *                   relevant settings
   * @param jobLockEventListener the {@link JobLock} event listener
   * @return an instance of {@link JobLock}
   * @throws JobLockException throw when the {@link JobLock} fails to initialize
   */
  public static JobLock getJobLock(Properties properties, JobLockEventListener jobLockEventListener)
          throws JobLockException {
    Preconditions.checkNotNull(properties);
    Preconditions.checkNotNull(jobLockEventListener);
    JobLock jobLock;
    if (properties.containsKey(ConfigurationKeys.JOB_LOCK_TYPE)) {
      try {
        Class<?> jobLockClass = Class.forName(
            properties.getProperty(ConfigurationKeys.JOB_LOCK_TYPE, FileBasedJobLock.class.getName()));
        jobLock = (JobLock) ConstructorUtils.invokeConstructor(jobLockClass, properties);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException |
          NoSuchMethodException | InvocationTargetException e) {
        throw new JobLockException(e);
      }
    } else {
      jobLock = new FileBasedJobLock(properties);
    }
    if (jobLock instanceof ListenableJobLock) {
      ((ListenableJobLock)jobLock).setEventListener(jobLockEventListener);
    }
    return jobLock;
  }
}
