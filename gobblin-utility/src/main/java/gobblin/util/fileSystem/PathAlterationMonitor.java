package gobblin.util.filesystem;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public final class PathAlterationMonitor implements Runnable {
  private final long interval;
  private volatile boolean running = false;
  private Thread thread = null;
  private ThreadFactory threadFactory;
  private ScheduledExecutorService executor;
  private ScheduledFuture<?> executionResult;
  private final List<PathAlterationObserver> observers = new CopyOnWriteArrayList<PathAlterationObserver>();

  // Parameter for the running the Monitor periodically.
  private int initialDelay = 0;

  public PathAlterationMonitor() {
    this(10000);
    this.executor = Executors.newScheduledThreadPool(1);
  }

  public PathAlterationMonitor(final long interval) {
    this.interval = interval;
    this.executor = Executors.newScheduledThreadPool(1);
  }

  /**
   * Set the thread factory.
   * For specifying priority, naming conventions, etc.
   * @param threadFactory the thread factory
   */
  public synchronized void setThreadFactory(ThreadFactory threadFactory) {
    this.threadFactory = threadFactory;
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
    for (final PathAlterationObserver observer : observers) {
      observer.initialize();
    }
    running = true;

    if (threadFactory != null) {
      thread = threadFactory.newThread(this);
    } else {
      thread = new Thread(this);
    }

    executionResult = executor.scheduleWithFixedDelay(thread, initialDelay, interval, TimeUnit.MILLISECONDS);
  }

  /**
   * Stop monitoring, shutdowm the service
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void stop()
      throws Exception {
    stop(interval);
    executor.shutdown();
  }

  /**
   * Stop monitoring but don't shutdown the service.
   *
   * @param stopInterval the amount of time in milliseconds to wait for the thread to finish.
   * A value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
   * @throws Exception if an error occurs initializing the observer
   * @since 2.1
   */
  public synchronized void stop(final long stopInterval)
      throws Exception {
    if (running == false) {
      throw new IllegalStateException("Monitor is not running");
    }
    executionResult.cancel(true) ;
    running = false;
    for (final PathAlterationObserver observer : observers) {
      observer.destroy();
    }
  }

  @Override
  public void run() {
    while (running) {
      for (final PathAlterationObserver observer : observers) {
        try {
          observer.checkAndNotify();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (!running) {
        break;
      }
    }
  }
}