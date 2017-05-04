package gobblin.async;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import javax.annotation.concurrent.ThreadSafe;


/**
 * Base class with skeleton logic to dispatch a record asynchronously. It buffers the records and consumes
 * them by {@link #run()}
 *
 * <p>
 *   However the records are consumed depends on the actual implementation of {@link #dispatch(Queue)}, which
 *   may process one record or a batch at a time
 * </p>
 *
 * @param <D> type of record
 */
@ThreadSafe
public abstract class AsyncDataDispatcher<D> extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDataDispatcher.class);

  // Queue to buffer records
  private final BlockingQueue<D> buffer;

  // Lock for isBufferEmpty condition
  private final Lock lock;
  private final Condition isBufferEmpty;

  // Indicate a buffer empty occurrence
  private boolean isBufferEmptyOccurred;

  public AsyncDataDispatcher(int capacity) {
    super();
    buffer = new ArrayBlockingQueue<>(capacity);
    lock = new ReentrantLock(true);
    isBufferEmpty = lock.newCondition();
    isBufferEmptyOccurred = false;
    startAsync();
    awaitRunning();
  }

  /**
   * Synchronously dispatch records in the buffer. Retries should be done if necessary. Every record
   * consumed from the buffer must have its callback called if any.
   *
   * @param buffer the buffer which contains a collection of records
   * @throws DispatchException if dispatch failed
   */
  protected abstract void dispatch(Queue<D> buffer)
      throws DispatchException;

  protected void put(D record) {
    // Check if dispatcher is running to accept new record
    checkRunning("put");
    try {
      buffer.put(record);
      // Check after a successful blocking put
      checkRunning("put");
    } catch (InterruptedException e) {
      throw new RuntimeException("Waiting to put a record interrupted", e);
    } catch (RuntimeException e) {
      // Clear the buffer to wake up other threads waiting on put
      buffer.clear();
      throw e;
    }
  }

  @Override
  protected void run()
      throws Exception {
    LOG.info("Start processing records");
    // A main loop to process records
    while (true) {
      while (buffer.size() == 0) {
        // Buffer is empty
        notifyBufferEmptyOccurrence();
        if (!isRunning()) {
          // Clean return
          return;
        }
        // Waiting for some time to get some records
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          LOG.debug("RecordProcessor sleep interrupted", e);
        }
      }

      // Remove the old buffer empty occurrence
      try {
        lock.lock();
        isBufferEmptyOccurred = false;
      } finally {
        lock.unlock();
      }

      // Dispatch records
      try {
        dispatch(buffer);
      } catch (DispatchException e) {
        LOG.error("Dispatch incurs an exception", e);
        if (e.isFatal()) {
          // Mark stopping
          stopAsync();
          // Drain the buffer
          buffer.clear();
          // Wake up the threads waiting on buffer empty occurrence
          notifyBufferEmptyOccurrence();
          throw e;
        }
      }
    }
  }

  public void terminate() {
    stopAsync().awaitTerminated();
  }

  protected void checkRunning(String forWhat) {
    if (!isRunning()) {
      RuntimeException e = new RuntimeException("Attempt to operate when writer is " + state().name());
      LOG.error(forWhat, e);
      throw e;
    }
  }

  protected void waitForABufferEmptyOccurrence() {
    checkRunning("waitForABufferEmptyOccurrence");
    try {
      lock.lock();
      // Waiting for a buffer empty occurrence
      while (!isBufferEmptyOccurred) {
        try {
          isBufferEmpty.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Waiting for buffer flush interrupted", e);
        }
      }
      // Remove the consumed buffer empty occurrence
      isBufferEmptyOccurred = false;
    } finally {
      lock.unlock();
      checkRunning("waitForABufferEmptyOccurrence");
    }
  }

  private void notifyBufferEmptyOccurrence() {
    try {
      lock.lock();
      isBufferEmptyOccurred = true;
      isBufferEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
