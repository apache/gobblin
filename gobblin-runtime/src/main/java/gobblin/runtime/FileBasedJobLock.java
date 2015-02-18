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

import java.io.IOException;

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
 * @author ynli
 */
public class FileBasedJobLock implements JobLock {

  public static final String LOCK_FILE_EXTENSION = ".lock";

  private final FileSystem fs;
  // Empty file associated with the lock
  private final Path lockFile;

  public FileBasedJobLock(FileSystem fs, String lockFileDir, String jobName)
      throws IOException {

    this.fs = fs;
    this.lockFile = new Path(lockFileDir, jobName + LOCK_FILE_EXTENSION);
  }

  @Override
  public void lock()
      throws IOException {
    if (!this.fs.createNewFile(this.lockFile)) {
      throw new IOException("Failed to create lock file " + this.lockFile.getName());
    }
  }

  @Override
  public void unlock()
      throws IOException {
    if (!isLocked()) {
      return;
    }

    this.fs.delete(this.lockFile, false);
  }

  @Override
  public boolean tryLock()
      throws IOException {
    return this.fs.createNewFile(this.lockFile);
  }

  @Override
  public boolean isLocked()
      throws IOException {
    return this.fs.exists(this.lockFile);
  }
}
