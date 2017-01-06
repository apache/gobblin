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

/**
 *
 */
package gobblin.runtime.locks;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.HadoopUtils;

/**
 * A factory for file-based job locks
 */
public class FileBasedJobLockFactory  {

  private final FileSystem fs;
  private final String lockFileDir;

  /** */
  public FileBasedJobLockFactory(FileSystem fs, String lockFileDir) {
    this.fs = fs;
    this.lockFileDir = lockFileDir;
  }

  Path getLockFile(String jobName) {
    return new Path(lockFileDir, jobName + FileBasedJobLock.LOCK_FILE_EXTENSION);
  }

  /**
   * Acquire the lock.
   *
   * @throws JobLockException thrown if the {@link JobLock} fails to be acquired
   */
  void lock(Path lockFile) throws JobLockException {
    try {
      if (!this.fs.createNewFile(lockFile)) {
        throw new JobLockException("Failed to create lock file " + lockFile.getName());
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
  void unlock(Path lockFile) throws JobLockException {
    if (!isLocked(lockFile)) {
      return;
    }

    try {
      this.fs.delete(lockFile, false);
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
  boolean tryLock(Path lockFile) throws JobLockException {
    try {
      return this.fs.createNewFile(lockFile);
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
  boolean isLocked(Path lockFile) throws JobLockException {
    try {
      return this.fs.exists(lockFile);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

  public static FileBasedJobLockFactory createForProperties(Properties properties)
         throws JobLockException {
    try {
      FileSystem fs = FileSystem.get(
          URI.create(properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI)),
          HadoopUtils.getConfFromProperties(properties));
      String lockFileDir = properties.getProperty(FileBasedJobLock.JOB_LOCK_DIR);
      return new FileBasedJobLockFactory(fs, lockFileDir);
    } catch (IOException e) {
      throw new JobLockException(e);
    }
  }

}
