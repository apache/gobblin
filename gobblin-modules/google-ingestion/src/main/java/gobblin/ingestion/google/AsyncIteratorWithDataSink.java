package gobblin.ingestion.google;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class AsyncIteratorWithDataSink<T> implements Iterator<T> {
  private final Object lock = new Object();
  private volatile Throwable exceptionInProducerThread = null;
  private Thread _producerThread;
  protected LinkedBlockingDeque<T> _dataSink = new LinkedBlockingDeque<>(2000);

  @Override
  public boolean hasNext() {
    initialize();
    if (!_dataSink.isEmpty()) {
      return true;
    }
    try {
      T next = _dataSink.poll(1, TimeUnit.SECONDS);
      while (next == null) {
        if (_producerThread.isAlive()) {
          //Job not done yet. Keep waiting...
          next = _dataSink.poll(1, TimeUnit.SECONDS);
        } else {
          synchronized (lock) {
            if (exceptionInProducerThread != null) {
              throw new RuntimeException(
                  String.format("Found exception in producer thread %s", _producerThread.getName()),
                  exceptionInProducerThread);
            }
          }
          log.info("Producer job has finished. No more query data in the queue.");
          return false;
        }
      }
      //Must put it back. Implement in this way because LinkedBlockingDeque doesn't support blocking peek.
      _dataSink.putFirst(next);
      return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initialize() {
    if (_producerThread == null) {
      _producerThread = new Thread(getProducerRunnable());
      _producerThread.setUncaughtExceptionHandler(getExceptionHandler());
      _producerThread.start();
    }
  }

  protected abstract Runnable getProducerRunnable();

  @Override
  public T next() {
    if (hasNext()) {
      return _dataSink.remove();
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private Thread.UncaughtExceptionHandler getExceptionHandler() {
    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        synchronized (lock) {
          exceptionInProducerThread = e;
        }
      }
    };
  }
}
