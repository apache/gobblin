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

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * An implementation of {@link JobLock} that relies on new file creation on a file system.
 *
 * <p>
 *     Acquiring a lock is done by creating a new empty file on the file system and
 *     releasing a lock is done by deleting the empty file associated with the lock.
 * </p>
 *
 * @author Yinan Li
 */
public class FileBasedJobLock implements JobLock {
  public static final String JOB_LOCK_DIR = "job.lock.dir";
  public static final String LOCK_FILE_EXTENSION = ".lock";

  private FileSystem fs;
  // Empty file associated with the lock
  private Path lockFile;

  /**
   * Initializes the lock.
   *
   * @param properties  the job properties
   * @param jobLockEventListener the listener for lock events
   * @throws JobLockException thrown if the {@link JobLock} fails to initialize
   */
  @Override
  public void initialize(Properties properties, JobLockEventListener jobLockEventListener) throws JobLockException {
    try {
      this.fs = FileSystem.get(
              URI.create(properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI)),
              HadoopUtils.getConfFromProperties(properties));
      String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY);
      String lockFileDir = properties.getProperty(JOB_LOCK_DIR);
      this.lockFile = new Path(lockFileDir, jobName + LOCK_FILE_EXTENSION);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  /**
   * Acquire the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  @Override
  public void lock() throws JobLockException {
    try {
      if (!this.fs.createNewFile(this.lockFile)) {
        throw new JobLockException("Failed to create lock file " + this.lockFile.getName());
      }
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  /**
   * Release the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be released
   */
  @Override
  public void unlock() throws JobLockException {
    if (!isLocked()) {
      return;
    }

    try {
      this.fs.delete(this.lockFile, false);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  /**
   * Try locking the lock.
   *
   * @return <em>true</em> if the lock is successfully locked,
   *         <em>false</em> if otherwise.
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  @Override
  public boolean tryLock() throws JobLockException {
    try {
      return this.fs.createNewFile(this.lockFile);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws JobLockException thrown if checking the status of the {@link JobLock} fails
   */
  @Override
  public boolean isLocked() throws JobLockException {
    try {
      return this.fs.exists(this.lockFile);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
  }
}
