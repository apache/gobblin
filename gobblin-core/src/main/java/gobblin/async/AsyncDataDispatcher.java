package gobblin.async;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;


/**
 * Base class with skeleton logic to dispatch a record asynchronously. It buffers the records and consumes
 * them by {@link RecordProcessor}
 *
 * <p>
 *   However the records are consumed depends on the actual implementation of {@link #dispatch(Queue)}, which
 *   may process one record or a batch at a time
 * </p>
 *
 * @param <D> type of record
 */
@ThreadSafe
public abstract class AsyncDataDispatcher<D> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDataDispatcher.class);

  // Queue to batch records
  private final BlockingQueue<D> buffer;

  private final ExecutorService service;
  private final Future<?> recordProcessorFuture;

  public enum ProcessorState {
    RUNNING,
    CLOSING,
    TERMINATED
  }

  private volatile ProcessorState state;

  // Lock for isFlushed condition
  private final Lock lock;
  // isFlushed is signaled when buffer is empty at sometime
  private final Condition isFlushed;

  public AsyncDataDispatcher(int capacity) {
    service = Executors.newFixedThreadPool(1);
    recordProcessorFuture = service.submit(new RecordProcessor());
    buffer = new ArrayBlockingQueue<>(capacity);
    state = ProcessorState.RUNNING;
    lock = new ReentrantLock(true);
    isFlushed = lock.newCondition();
  }

  /**
   * Synchronously dispatch records in the buffer. Retries should be done if necessary. Every record
   * consumed from the buffer must have its callback called if any.
   *
   * @param buffer the buffer which contains a collection of records
   * @throws DispatchException if dispatch failed
   */
  protected abstract void dispatch(Queue<D> buffer) throws DispatchException;

  protected void put(D record) {
    if (state != ProcessorState.RUNNING) {
      RuntimeException e = new RuntimeException("Attempt to write data when writer is " + state.name());
      LOG.error("Write rejected", e);
      throw e;
    }

    while (true) {
      try {
        buffer.put(record);
        break;
      } catch (InterruptedException e) {
        LOG.debug("Waiting to put a record interrupted", e);
      }
    }
  }

  @Override
  public void close()
      throws IOException {
    flush();
    state = ProcessorState.CLOSING;
    try {
      recordProcessorFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.info("Waiting for RecordProcessor returns an exception", e);
    }
    service.shutdown();
  }

  public void flush()
      throws IOException {
  }

  private void notifyFlushed() {
    lock.lock();
    isFlushed.signalAll();
    lock.unlock();
  }

  private class RecordProcessor implements Runnable {
    public void run() {
      LOG.info("Start processing records");
      /*
       * A main loop to process records
       */
      while (true) {
        if (buffer.size() == 0) {
          notifyFlushed();
          if (state == ProcessorState.CLOSING) {
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

        // Dispatch records
        try {
          dispatch(buffer);
        } catch (DispatchException e) {
          LOG.error("Dispatch incurs an exception", e);
          if (e.isFatal()) {
            state = ProcessorState.TERMINATED;
            notifyFlushed();
            throw new RuntimeException("RecordProcessor terminated on exception", e);
          }
        }
      }
    }
  }
}
