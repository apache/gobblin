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
package gobblin.runtime.locks;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobSpec;
import gobblin.util.HadoopUtils;

/**
 * A factory for file-based job locks. All locks are presented as files under a common directory.
 * If the directory does not exist, it will be automatically created and removed on close().
 */
public class FileBasedJobLockFactory implements JobLockFactory<FileBasedJobLock> {

  /** The URI of the file system with the directory for lock files*/
  public static final String FS_URI_CONFIG = "fsURI";
  /** The path to the directory for lock files*/
  public static final String LOCK_DIR_CONFIG = "lockDir";

  static final String DEFAULT_LOCK_DIR_PREFIX = "/tmp/gobblin-job-locks-";

  /**
   * Default waiting period (5 minutes).
   * TODO add configuration support
   */
  static final long DEFAULT_WAIT_MS = 300000;

  private final FileSystem fs;
  private final Path lockFileDir;
  private final Logger log;
  private final boolean deleteLockDirOnClose;

  /** Constructs a new factory
   * @throws IOException */
  public FileBasedJobLockFactory(FileSystem fs, String lockFileDir, Optional<Logger> log)
         throws IOException {
    this.fs = fs;
    this.lockFileDir = new Path(lockFileDir);
    this.log = log.or(LoggerFactory.getLogger(getClass().getName() + "-" + lockFileDir));
    this.deleteLockDirOnClose = !this.fs.exists(this.lockFileDir);
    if (deleteLockDirOnClose) {
      createLockDir(this.fs, this.lockFileDir);
    }
  }

  public FileBasedJobLockFactory(FileSystem fs, String lockFileDir) throws IOException {
    this(fs, lockFileDir, Optional.<Logger>absent());
  }

  /** Create a new instance using the specified factory and hadoop configurations. */
  public static FileBasedJobLockFactory create(Config factoryConfig,
                                        Configuration hadoopConf,
                                        Optional<Logger> log)
         throws IOException {
    FileSystem fs = factoryConfig.hasPath(FS_URI_CONFIG) ?
            FileSystem.get(URI.create(factoryConfig.getString(FS_URI_CONFIG)), hadoopConf) :
            getDefaultFileSystem(hadoopConf);
    String lockFilesDir =  factoryConfig.hasPath(LOCK_DIR_CONFIG) ?
           factoryConfig.getString(LOCK_DIR_CONFIG) :
           getDefaultLockDir(fs, log);
    return new FileBasedJobLockFactory(fs, lockFilesDir, log);
  }

  public static FileSystem getDefaultFileSystem(Configuration hadoopConf) throws IOException {
    return FileSystem.getLocal(hadoopConf);
  }

  public static String getDefaultLockDir(FileSystem fs, Optional<Logger> log) {
    Random rng = new Random();
    Path dirName;
    try
    {
      do {
        dirName = new Path(DEFAULT_LOCK_DIR_PREFIX + rng.nextLong());
      } while (fs.exists(dirName));
    } catch (IllegalArgumentException | IOException e) {
      throw new RuntimeException("Unable to create job lock directory: " + e, e);
    }
    if (log.isPresent()) {
      log.get().info("Created default job lock directory: " + dirName);
    }
    return dirName.toString();
  }

  protected static void createLockDir(FileSystem fs, Path dirName) throws IOException {
    if (!fs.mkdirs(dirName, getDefaultDirPermissions())) {
      throw new RuntimeException("Unable to create job lock directory: " + dirName);
    }
  }

  protected static FsPermission getDefaultDirPermissions() {
    return new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE);
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
    log.debug("Creating lock: {}", lockFile);
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
    log.debug("Removing lock: {}", lockFile);
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
    log.debug("Attempting lock: {}", lockFile);
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

  public static Config getConfigForProperties(Properties properties) {
    return ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
       .put(FS_URI_CONFIG, properties.getProperty(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI))
       .put(LOCK_DIR_CONFIG, properties.getProperty(FileBasedJobLock.JOB_LOCK_DIR))
       .build());
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

  @Override
  public FileBasedJobLock getJobLock(JobSpec jobSpec) throws TimeoutException {
    String jobName = getJobName(jobSpec);
    return new FileBasedJobLock(jobName, this);
  }

  @VisibleForTesting
  static String getJobName(JobSpec jobSpec) {
    return jobSpec.getUri().toString().replaceAll("[/.:]", "_");
  }

  @VisibleForTesting
  FileSystem getFs() {
    return fs;
  }

  @VisibleForTesting
  Path getLockFileDir() {
    return lockFileDir;
  }

  @Override
  public void close() throws IOException {
    if (this.deleteLockDirOnClose) {
      this.log.info("Delete auto-created lock directory: {}", getLockFileDir());
      if (!this.fs.delete(getLockFileDir(), true)) {
        this.log.warn("Failed to delete lock directory: {}", getLockFileDir());
      }
    }
  }

}
