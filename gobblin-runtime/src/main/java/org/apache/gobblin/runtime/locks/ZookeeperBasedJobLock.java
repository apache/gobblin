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
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;

import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link JobLock} that uses Zookeeper.
 *
 * @author Joel Baranick
 */
@Slf4j
public class ZookeeperBasedJobLock implements ListenableJobLock {
  private static final String LOCKS_ROOT_PATH = "/locks";
  private static final String CONNECTION_STRING_DEFAULT = "localhost:2181";
  private static final int LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS_DEFAULT = 5000;
  private static final int CONNECTION_TIMEOUT_SECONDS_DEFAULT = 30;
  private static final int SESSION_TIMEOUT_SECONDS_DEFAULT = 180;
  private static final int RETRY_BACKOFF_SECONDS_DEFAULT = 1;
  private static final int MAX_RETRY_COUNT_DEFAULT = 10;
  private static CuratorFramework curatorFramework;
  private static ConcurrentMap<String, JobLockEventListener> lockEventListeners = Maps.newConcurrentMap();
  private static Thread curatorFrameworkShutdownHook;

  public static final String LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS = "gobblin.locks.zookeeper.acquire.timeout_milliseconds";
  public static final String CONNECTION_STRING = "gobblin.locks.zookeeper.connection_string";
  public static final String CONNECTION_TIMEOUT_SECONDS = "gobblin.locks.zookeeper.connection.timeout_seconds";
  public static final String SESSION_TIMEOUT_SECONDS = "gobblin.locks.zookeeper.session.timeout_seconds";
  public static final String RETRY_BACKOFF_SECONDS = "gobblin.locks.zookeeper.retry.backoff_seconds";
  public static final String MAX_RETRY_COUNT = "gobblin.locks.zookeeper.retry.max_count";

  private String lockPath;
  private long lockAcquireTimeoutMilliseconds;
  private InterProcessLock lock;

  public ZookeeperBasedJobLock(Properties properties) throws JobLockException {
    String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    this.lockAcquireTimeoutMilliseconds =
        getLong(properties, LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS, LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS_DEFAULT);
    this.lockPath = Paths.get(LOCKS_ROOT_PATH, jobName).toString();
    initializeCuratorFramework(properties);
    lock = new InterProcessSemaphoreMutex(curatorFramework, lockPath);
  }

  /**
   * Sets the job lock listener.
   *
   * @param jobLockEventListener the listener for lock events
   */
  @Override
  public void setEventListener(JobLockEventListener jobLockEventListener) {
    lockEventListeners.putIfAbsent(this.lockPath, jobLockEventListener);
  }

  /**
   * Acquire the lock.
   *
   * @throws IOException
   */
  @Override
  public void lock() throws JobLockException {
    try {
      this.lock.acquire();
    } catch (Exception e) {
      throw new JobLockException("Failed to acquire lock " + this.lockPath, e);
    }
  }

  /**
   * Release the lock.
   *
   * @throws IOException
   */
  @Override
  public void unlock() throws JobLockException {
    if (this.lock.isAcquiredInThisProcess()) {
      try {
        this.lock.release();
      } catch (Exception e) {
        throw new JobLockException("Failed to release lock " + this.lockPath, e);
      }
    }
  }

  /**
   * Try locking the lock.
   *
   * @return <em>true</em> if the lock is successfully locked,
   *         <em>false</em> if otherwise.
   * @throws IOException
   */
  @Override
  public boolean tryLock() throws JobLockException {
    try {
      return this.lock.acquire(lockAcquireTimeoutMilliseconds, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new JobLockException("Failed to acquire lock " + this.lockPath, e);
    }
  }

  /**
   * Check if the lock is locked.
   *
   * @return if the lock is locked
   * @throws IOException
   */
  @Override
  public boolean isLocked() throws JobLockException {
    return this.lock.isAcquiredInThisProcess();
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
    try {
      this.unlock();
    } catch (JobLockException e) {
      throw new IOException(e);
    } finally {
      lockEventListeners.remove(this.lockPath);
    }
  }

  private synchronized static void initializeCuratorFramework(Properties properties) {
    if (curatorFrameworkShutdownHook == null) {
      curatorFrameworkShutdownHook = new CuratorFrameworkShutdownHook();
      Runtime.getRuntime().addShutdownHook(curatorFrameworkShutdownHook);
    }
    if (curatorFramework == null) {
      CuratorFramework newCuratorFramework = CuratorFrameworkFactory.builder()
              .connectString(properties.getProperty(CONNECTION_STRING, CONNECTION_STRING_DEFAULT))
              .connectionTimeoutMs(
                      getMilliseconds(properties, CONNECTION_TIMEOUT_SECONDS, CONNECTION_TIMEOUT_SECONDS_DEFAULT))
              .sessionTimeoutMs(
                      getMilliseconds(properties, SESSION_TIMEOUT_SECONDS, SESSION_TIMEOUT_SECONDS_DEFAULT))
              .retryPolicy(new ExponentialBackoffRetry(
                      getMilliseconds(properties, RETRY_BACKOFF_SECONDS, RETRY_BACKOFF_SECONDS_DEFAULT),
                      getInt(properties, MAX_RETRY_COUNT, MAX_RETRY_COUNT_DEFAULT)))
              .build();

      newCuratorFramework.getConnectionStateListenable().addListener(new ConnectionStateListener() {
          @Override
          public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            switch (connectionState) {
              case LOST:
                log.warn("Lost connection with zookeeper");
                for (Map.Entry<String, JobLockEventListener> lockEventListener : lockEventListeners.entrySet()) {
                  log.warn("Informing job {} that lock was lost", lockEventListener.getKey());
                  lockEventListener.getValue().onLost();
                }
                break;
              case SUSPENDED:
                log.warn("Suspended connection with zookeeper");
                for (Map.Entry<String, JobLockEventListener> lockEventListener : lockEventListeners.entrySet()) {
                  log.warn("Informing job {} that lock was suspended", lockEventListener.getKey());
                  lockEventListener.getValue().onLost();
                }
                break;
              case CONNECTED:
                log.info("Connected with zookeeper");
                break;
              case RECONNECTED:
                log.warn("Regained connection with zookeeper");
                break;
              case READ_ONLY:
                log.warn("Zookeeper connection went into read-only mode");
                break;
            }
          }
      });
      newCuratorFramework.start();
      try {
        if (!newCuratorFramework.blockUntilConnected(
                getInt(properties, CONNECTION_TIMEOUT_SECONDS, CONNECTION_TIMEOUT_SECONDS_DEFAULT),
                TimeUnit.SECONDS)) {
          throw new RuntimeException("Time out while waiting to connect to zookeeper");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting to connect to zookeeper");
      }
      curatorFramework = newCuratorFramework;
    }
  }

  @VisibleForTesting
  static synchronized void shutdownCuratorFramework() {
    if (curatorFramework != null) {
      curatorFramework.close();
      curatorFramework = null;
    }
  }

  private static int getInt(Properties properties, String key, int defaultValue) {
    return Integer.parseInt(properties.getProperty(key, Integer.toString(defaultValue)));
  }

  private static long getLong(Properties properties, String key, long defaultValue) {
    return Long.parseLong(properties.getProperty(key, Long.toString(defaultValue)));
  }

  private static int getMilliseconds(Properties properties, String key, int defaultValue) {
    return getInt(properties, key, defaultValue) * 1000;
  }

  private static class CuratorFrameworkShutdownHook extends Thread {
    public void run() {
      log.info("Shutting down curator framework...");
      try {
        shutdownCuratorFramework();
        log.info("Curator framework shut down.");
      } catch (Exception e) {
        log.error("Error while shutting down curator framework.", e);
      }
    }
  }
}
