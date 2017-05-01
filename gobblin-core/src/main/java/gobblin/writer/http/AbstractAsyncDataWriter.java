package gobblin.writer.http;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import gobblin.writer.AsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


@ThreadSafe
public abstract class AbstractAsyncDataWriter<D> implements AsyncDataWriter<D> {
  public static int DEFAULT_BUFFER_CAPACITY = 10000;
  private static final Logger LOG = LoggerFactory.getLogger(AbstractAsyncDataWriter.class);

  /** Main lock guarding all access */
  private final ReentrantLock lock;

  /** Condition for waiting takes */
  private final Condition notEmpty;

  /** Condition for waiting empty */
  private final Condition isEmpty;

  /** Condition for waiting puts */
  private final Condition notFull;

  /** Queue to batch records */
  private final Queue<BufferedRecord<D>> buffer;

  /** Queue capacity */
  private final int capacity;

  private final ExecutorService service;
  private Future<?> recordProcessorFuture;

  private volatile boolean isClosing;

  public AbstractAsyncDataWriter(int capacity) {
    service = Executors.newFixedThreadPool(1);
    recordProcessorFuture = service.submit(new RecordProcessor());

    lock = new ReentrantLock();
    notEmpty = lock.newCondition();
    isEmpty = lock.newCondition();
    notFull =  lock.newCondition();
    buffer = new ArrayDeque<>(capacity);
    this.capacity = capacity;
    isClosing = false;
  }

  /**
   * Dispatch the record in the buffer. Retries should be done if necessary
   *
   * @param buffer The buffer which contains a collection of records
   * @return the number of records are successfully dispatched or -1 if dispatch failed
   */
  protected abstract int dispatch(Queue<BufferedRecord<D>> buffer) throws Throwable;

  private void put(BufferedRecord<D> record) {
    if (isClosing) {
      RuntimeException e = new RuntimeException("Attempt to write data when writer is closing");
      LOG.error("Invalid write", e);
      throw e;
    }

    lock.lock();
    while (buffer.size() == capacity) {
      try {
        notFull.await();
      } catch (InterruptedException e) {
        LOG.info("Waiting for queue not full interrupted", e);
      }
    }

    buffer.add(record);
    notEmpty.signalAll();
    lock.unlock();
  }

  @Override
  public Future<WriteResponse> write(D record, @Nullable WriteCallback callback) {
    if (service.isShutdown()) {
      RuntimeException e = new RuntimeException("Attempt to write a record after close.");
      LOG.error("Invalid write", e);
      throw e;
    }
    BufferedRecord<D> item = new BufferedRecord<>(record, callback);
    put(item);
    return null;
  }

  @Override
  public void close()
      throws IOException {
    isClosing = true;

    // Invoke RecordProcessor in case it sleeps
    lock.lock();
    notEmpty.signalAll();
    lock.unlock();

    try {
      recordProcessorFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.info("Waiting for record processor returns an exception", e);
    }
    service.shutdown();
  }

  @Override
  public void flush()
      throws IOException {
    lock.lock();
    while (buffer.size() != 0) {
      try {
        isEmpty.await();
      } catch (InterruptedException e) {
        LOG.info("Waiting for buffer empty interrupted", e);
      }
    }
    lock.unlock();
  }

  private class RecordProcessor implements Runnable {
    public void run() {
      LOG.info("Start processing records in the blocking queue");
      /*
       * A main loop to process records
       */
      while (true) {
        lock.lock();
        // Waiting for records to dispatch
        while (buffer.size() == 0) {
          if (isClosing) {
            lock.unlock();
            // Safely return after processing all records
            return;
          }
          try {
            notEmpty.await();
          } catch (InterruptedException e) {
            LOG.info("Waiting for queue not empty interrupted", e);
          }
        }

        // Dispatch records
        try {
          if (dispatch(buffer) < 0) {
            throw new IOException("Failed on dispatching");
          }
        } catch (Throwable throwable) {
          LOG.error("RecordProcessor breaks on exception", throwable);
          throw new RuntimeException("Exception on dispatching", throwable);
        } finally {
          // Signal threads waiting on flush
          if (buffer.size() == 0) {
            isEmpty.signalAll();
          }
          notFull.signalAll();
          lock.unlock();
        }
      }
    }
  }
}
