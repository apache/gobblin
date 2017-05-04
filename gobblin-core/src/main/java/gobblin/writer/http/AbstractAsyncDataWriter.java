package gobblin.writer.http;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import gobblin.async.AsyncDataDispatcher;
import gobblin.writer.AsyncDataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


/**
 * Base class to write data asynchronously. It is an {@link AsyncDataDispatcher} on {@link BufferedRecord}, which
 * wraps a record and its callback.
 *
 * @param <D> type of record
 */
@ThreadSafe
public abstract class AbstractAsyncDataWriter<D> extends AsyncDataDispatcher<BufferedRecord<D>> implements AsyncDataWriter<D> {
  public static final int DEFAULT_BUFFER_CAPACITY = 10000;

  public AbstractAsyncDataWriter(int capacity) {
    super(capacity);
  }

  /**
   * Asynchronously write the record with a callback. Asynchronous via {@link Future} is
   * not supported
   */
  @Override
  public Future<WriteResponse> write(D record, @Nullable WriteCallback callback) {
    BufferedRecord<D> bufferedRecord = new BufferedRecord<>(record, callback);
    put(bufferedRecord);
    return null;
  }

  @Override
  public void close()
      throws IOException {
    checkRunning("close");
    flush();
    terminate();
  }

  /**
   * Wait for a buffer empty occurrence
   */
  @Override
  public void flush()
      throws IOException {
    waitForABufferEmptyOccurrence();
  }
}
