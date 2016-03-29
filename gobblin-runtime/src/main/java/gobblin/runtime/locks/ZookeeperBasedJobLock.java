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
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;

import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link JobLock} that uses Zookeeper.
 *
 * @author Joel Baranick
 */
@Slf4j
public class ZookeeperBasedJobLock implements JobLock {
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

  public static final String LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS = "zookeeper.locks.acquire.timeout.milliseconds";
  public static final String CONNECTION_STRING = "zookeeper.connection.string";
  public static final String CONNECTION_TIMEOUT_SECONDS = "zookeeper.connection.timeout.seconds";
  public static final String SESSION_TIMEOUT_SECONDS = "zookeeper.session.timeout.seconds";
  public static final String RETRY_BACKOFF_SECONDS = "zookeeper.retry.backoff.seconds";
  public static final String MAX_RETRY_COUNT = "zookeeper.retry.count.max";

  private String lockPath;
  private long lockAcquireTimeoutMilliseconds;
  private InterProcessSemaphoreMutex lock;

    /**
   * Initializes the lock.
   *
   * @param properties  the job properties
   * @param jobLockEventListener the listener for lock events
   * @throws JobLockException thrown if the {@link JobLock} fails to initialize
   */
  @Override
  public void initialize(Properties properties, JobLockEventListener jobLockEventListener) throws JobLockException {
    String jobName = properties.getProperty(ConfigurationKeys.JOB_NAME_KEY);
    this.lockAcquireTimeoutMilliseconds =
            getLong(properties, LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS, LOCKS_ACQUIRE_TIMEOUT_MILLISECONDS_DEFAULT);
    this.lockPath = Paths.get(LOCKS_ROOT_PATH, jobName).toString();
    lockEventListeners.putIfAbsent(this.lockPath, jobLockEventListener);
    initializeCuratorFramework(properties);
    lock = new InterProcessSemaphoreMutex(curatorFramework, lockPath);
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
              case SUSPENDED:
                log.warn("Lost connection with zookeeper");
                for (Map.Entry<String, JobLockEventListener> lockEventListener : lockEventListeners.entrySet()) {
                  log.warn("Informing job %s that lock was lost", lockEventListener.getKey());
                    lockEventListener.getValue().onLost();
                }
                break;
              case CONNECTED:
                log.info("Connected with zookeeper");
                break;
              case RECONNECTED:
                log.warn("Regained connection with zookeeper");
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
