package gobblin.writer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.annotation.Alpha;


/**
 * An accumulator which groups multiple records into a batch
 * How batching strategy works depends on the real implementation
 * One way to do this is scanning all the internal batches through an iterator
 */
@Alpha
public abstract class BatchAccumulator<D> implements Iterable<Batch<D>> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchAccumulator.class);

  private volatile boolean closed = false;
  private CountDownLatch closeComplete;
  private final AtomicInteger appendsInProgress;

  protected BatchAccumulator() {
    this.appendsInProgress = new AtomicInteger(0);
    this.closeComplete = new CountDownLatch(1);
  }

  /**
   * Append a record to this accumulator
   * <p>
   *   This method should never fail unless there is an exception. A future object should always be returned
   *   which can be queried to see if this record has been completed (completion means the wrapped batch has been
   *   sent and received acknowledgement and callback has been invoked). Internally it tracks how many parallel appends
   *   are in progress by incrementing appendsInProgress counter. The real append logic is inside {@link BatchAccumulator#enqueue(Object, WriteCallback)}
   * </p>
   *
   *   @param record : record needs to be added
   *   @param callback : A callback which will be invoked when the whole batch gets sent and acknowledged
   *   @return A future object which contains {@link RecordMetadata}
   */
  public final Future<RecordMetadata> append (D record, WriteCallback callback) throws InterruptedException {
    appendsInProgress.incrementAndGet();
    try {
      if (this.closed) {
        throw new RuntimeException ("Cannot append after accumulator has been closed");
      }
      return this.enqueue(record, callback);
    } finally {
      appendsInProgress.decrementAndGet();
    }
  }

  public final void waitClose() {
    try {
      this.closeComplete.await();
    } catch (InterruptedException e) {
      LOG.error ("accumulator close is interrupted");
    }

    LOG.info ("accumulator is closed");
  }

  public boolean isClosed () {
    return closed;
  }

  /**
   * Add a record to this accumulator
   * <p>
   *   This method should never fail unless there is an exception. A future object should always be returned
   *   which can be queried to see if this record has been completed (completion means the wrapped batch has been
   *   sent and received acknowledgement and callback has been invoked).
   * </p>
   *
   *   @param record : record needs to be added
   *   @param callback : A callback which will be invoked when the whole batch gets sent and acknowledged
   *   @return A future object which contains {@link RecordMetadata}
   */
  public abstract Future<RecordMetadata> enqueue (D record, WriteCallback callback) throws InterruptedException;

  /**
   * Wait until all the incomplete batches to be acknowledged
   */
  public abstract void flush ();

  /**
   * When close is invoked, all new coming records will be rejected
   * Add a busy loop here to ensure all the ongoing appends are completed
   */
  public final void close () {
    closed = true;
    while (appendsInProgress.get() > 0) {
      LOG.info("Append is still going on, wait for a while");
    }
    this.closeComplete.countDown();
  }

  /**
   * Release some resource current batch is allocated
   */
  public abstract void deallocate (Batch<D> batch);
}
