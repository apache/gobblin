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

package org.apache.gobblin.runtime.locks;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import org.apache.gobblin.configuration.ConfigurationKeys;


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
  /** Legacy */
  public static final String JOB_LOCK_DIR = "job.lock.dir";
  public static final String LOCK_FILE_EXTENSION = ".lock";

  private final FileBasedJobLockFactory parent;
  private final Path lockFile;

  public FileBasedJobLock(Properties properties) throws JobLockException {
    this(properties.getProperty(ConfigurationKeys.JOB_NAME_KEY),
         FileBasedJobLockFactory.createForProperties(properties));
  }

  FileBasedJobLock(String jobName, FileBasedJobLockFactory parent) {
    this.parent = parent;
    this.lockFile = parent.getLockFile(jobName);
  }

    /**
   * Acquire the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  @Override
  public void lock() throws JobLockException {
    this.parent.lock(this.lockFile);
  }

  /**
   * Release the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be released
   */
  @Override
  public void unlock() throws JobLockException {
    this.parent.unlock(this.lockFile);
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
    return this.parent.tryLock(this.lockFile);
  }

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws JobLockException thrown if checking the status of the {@link JobLock} fails
   */
  @Override
  public boolean isLocked() throws JobLockException {
    return this.parent.isLocked(this.lockFile);
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

  @VisibleForTesting
  Path getLockFile() {
    return lockFile;
  }
}
