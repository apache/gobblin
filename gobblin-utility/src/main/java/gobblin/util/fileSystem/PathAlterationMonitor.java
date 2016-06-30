package gobblin.util.filesystem;

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.util.ExecutorsUtils;


/**
 * A runnable that spawns a monitoring thread triggering any
 * registered {@link PathAlterationObserver} at a specified interval.
 *
 * Based on {@link org.apache.commons.io.monitor.FileAlterationMonitor}, implemented monitoring
 * thread to periodically check the monitored file in thread pool.
 */

public final class PathAlterationMonitor implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PathAlterationMonitor.class);
  private final long interval;
  private volatile boolean running = false;
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
      ExecutorsUtils.newDaemonThreadFactory(Optional.of(LOGGER), Optional.of("newDaemonThreadFactory")));
  private ScheduledFuture<?> executionResult;
  private final List<PathAlterationObserver> observers = new CopyOnWriteArrayList<PathAlterationObserver>();

  // Parameter for the running the Monitor periodically.
  private int initialDelay = 0;

  public PathAlterationMonitor() {
    this(3000);
  }

  public PathAlterationMonitor(final long interval) {
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
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void start()
      throws Exception {
    if (running) {
      throw new IllegalStateException("Monitor is already running");
    }
    running = true;
    for (final PathAlterationObserver observer : observers) {
      observer.initialize();
    }

    executionResult = executor.scheduleWithFixedDelay(this, initialDelay, interval, TimeUnit.MILLISECONDS);
  }

  /**
   * Stop monitoring
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void stop()
      throws Exception {
    stop(interval);
  }

  /**
   * Stop monitoring
   *
   * @param stopInterval the amount of time in milliseconds to wait for the thread to finish.
   * A value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
   * @throws Exception if an error occurs initializing the observer
   * @since 2.1
   */
  public synchronized void stop(final long stopInterval)
      throws Exception {
    if (!running) {
      throw new IllegalStateException("Monitor is not running");
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
    for (final PathAlterationObserver observer : observers) {
      try {
        observer.checkAndNotify();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    if (!running) {
      return;
    }
  }
}