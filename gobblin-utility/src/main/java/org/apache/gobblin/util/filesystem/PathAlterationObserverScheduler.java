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

package gobblin.util.filesystem;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.util.ExecutorsUtils;


/**
 * A runnable that spawns a monitoring thread triggering any
 * registered {@link PathAlterationObserver} at a specified interval.
 *
 * Based on {@link org.apache.commons.io.monitor.FileAlterationMonitor}, implemented monitoring
 * thread to periodically check the monitored file in thread pool.
 */

public final class PathAlterationObserverScheduler implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PathAlterationObserverScheduler.class);
  private final long interval;
  private volatile boolean running = false;
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
      ExecutorsUtils.newDaemonThreadFactory(Optional.of(LOGGER), Optional.of("newDaemonThreadFactory")));
  private ScheduledFuture<?> executionResult;
  private final List<PathAlterationObserver> observers = new CopyOnWriteArrayList<PathAlterationObserver>();

  // Parameter for the running the Monitor periodically.
  private int initialDelay = 0;

  public PathAlterationObserverScheduler() {
    this(3000);
  }

  public PathAlterationObserverScheduler(final long interval) {
    this.interval = interval;
  }

  /**
   * Add a file system observer to this monitor.
   *
   * @param observer The file system observer to add
   */
  public void addObserver(final PathAlterationObserver observer) {
    if (observer != null) {
      observers.add(observer);
    }
  }

  /**
   * Remove a file system observer from this monitor.
   *
   * @param observer The file system observer to remove
   */
  public void removeObserver(final PathAlterationObserver observer) {
    if (observer != null) {
      while (observers.remove(observer)) {
      }
    }
  }

  /**
   * Returns the set of {@link PathAlterationObserver} registered with
   * this monitor.
   *
   * @return The set of {@link PathAlterationObserver}
   */
  public Iterable<PathAlterationObserver> getObservers() {
    return observers;
  }

  /**
   * Start monitoring.
   * @throws IOException if an error occurs initializing the observer
   */
  public synchronized void start() throws IOException {
    if (running) {
      throw new IllegalStateException("Monitor is already running");
    }
    for (final PathAlterationObserver observer : observers) {
      observer.initialize();
    }

    if (interval > 0) {
        running = true;
        this.executionResult = executor.scheduleWithFixedDelay(this, initialDelay, interval, TimeUnit.MILLISECONDS);
    } else {
        LOGGER.info("Not starting due to non-positive scheduling interval:" + interval);
    }
  }

  /**
   * Stop monitoring
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void stop()
      throws IOException, InterruptedException {
    stop(interval);
  }

  /**
   * Stop monitoring
   *
   * @param stopInterval the amount of time in milliseconds to wait for the thread to finish.
   * A value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
   * @throws IOException if an error occurs initializing the observer
   * @since 2.1
   */
  public synchronized void stop(final long stopInterval)
      throws IOException, InterruptedException {
    if (!running) {
      LOGGER.warn("Already stopped");
      return;
    }
    running = false;


    for (final PathAlterationObserver observer : observers) {
      observer.destroy();
    }

    executionResult.cancel(true);
    executor.shutdown();
    if (!executor.awaitTermination(stopInterval, TimeUnit.MILLISECONDS)) {
      throw new RuntimeException("Did not shutdown in the timeout period");
    }
  }

  @Override
  public void run() {
     if (!running) {
         return;
     }
    for (final PathAlterationObserver observer : observers) {
      try {
        observer.checkAndNotify();
      } catch (IOException ioe) {
        LOGGER.error("Path alteration detector error.", ioe);
      }
    }
  }

  /**
   * Create and attach {@link PathAlterationObserverScheduler}s for the given
   * root directory and any nested subdirectories under the root directory to the given
   * {@link PathAlterationObserverScheduler}.
   * @param detector  a {@link PathAlterationObserverScheduler}
   * @param listener a {@link gobblin.util.filesystem.PathAlterationListener}
   * @param observerOptional Optional observer object. For testing routine, this has been initialized by user.
   *                         But for general usage, the observer object is created inside this method.
   * @param rootDirPath root directory
   * @throws IOException
   */
  public void addPathAlterationObserver(PathAlterationListener listener,
      Optional<PathAlterationObserver> observerOptional, Path rootDirPath)
      throws IOException {
    PathAlterationObserver observer = observerOptional.or(new PathAlterationObserver(rootDirPath));
    observer.addListener(listener);
    addObserver(observer);
  }

}
